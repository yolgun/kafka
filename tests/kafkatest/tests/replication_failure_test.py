# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ducktape.tests.test import Test
from ducktape.utils.util import wait_until
from ducktape.mark import matrix
from ducktape.mark import parametrize

from kafkatest.services.zookeeper import ZookeeperService
from kafkatest.services.kafka import KafkaService
from kafkatest.services.verifiable_producer import VerifiableProducer
from kafkatest.services.console_consumer import ConsoleConsumer

import signal
import time


"""
3 brokers, 3 topics, 3 partitions, 3 replicas on each topic
min.insync.replicas == 2
ack -1
failures: [leader, follower, controller] X [clean, hard, soft]
One test cases with with ack 1
One test case with compression toggled on

"""

# Failure types
CLEAN_BOUNCE = "clean_bounce"
HARD_BOUNCE = "hard_bounce"
SOFT_BOUNCE = "soft_bounce"

CLEAN_KILL = "clean_kill"
HARD_KILL = "hard_kill"
SOFT_KILL = "soft_kill"

# node types
LEADER = "leader"
# FOLLOWER = "follower"
CONTROLLER = "controller"


class ReplicationTest(Test):
    """Replication tests.
    These tests verify that replication provides simple durability guarantees by checking that data acked by
    brokers is still available for consumption in the face of various failure scenarios."""

    def __init__(self, test_context):
        super(ReplicationTest, self).__init__(test_context=test_context)

        self.replication_factor = 3
        self.zk = ZookeeperService(test_context, num_nodes=1)
        self.topics = {"topic1": {
                            "partitions": 3,
                            "replication-factor": self.replication_factor,
                            "min.insync.replicas": 2},
                       "topic2": {
                            "partitions": 3,
                            "replication-factor": self.replication_factor,
                            "min.insync.replicas": 2},
                       "topic3": {
                            "partitions": 3,
                            "replication-factor": self.replication_factor,
                            "min.insync.replicas": 2}
                    }
        self.topic_names = ["topic1", "topic2", "topic3"]
        self.topic_to_fail = "topic1"  # We'll induce failures on this topic

        self.kafka = KafkaService(test_context, num_nodes=3, zk=self.zk, topics=self.topics)
        self.producer_throughput = 100000
        self.num_producers = 3
        self.num_consumers = 3

        self.producers = [VerifiableProducer(self.test_context, 1, self.kafka, topic, throughput=self.producer_throughput)
                          for topic in self.topic_names]
        self.consumers = [ConsoleConsumer(self.test_context, 1, self.kafka, topic, consumer_timeout_ms=3000)
                          for topic in self.topic_names]

    def setUp(self):
        self.zk.start()
        self.kafka.start()

    """
    TODO: variable ack
    TODO: variable compression
    TODO: actually verify created topics in kafka.py
    """

    # @parametrize(ack=1)
    # @parametrize(compression=True)
    @matrix(failure=[SOFT_BOUNCE, HARD_BOUNCE, CLEAN_BOUNCE])
    @matrix(node_type=[CONTROLLER, LEADER])
    def test_replication(self, failure="clean", node_type="leader", ack=-1, compression=False, num_bounce=1):
        """This is the top-level test template.

        The steps are:
            Produce messages in the background while driving some failure condition
            When done driving failures, immediately stop producing
            Consume all messages
            Validate that messages acked by brokers were consumed

        Note that consuming is a bit tricky, at least with console consumer. The goal is to consume all messages
        (foreach partition) in the topic. In this case, waiting for the last message may cause the consumer to stop
        too soon since console consumer is consuming multiple partitions from a single thread and therefore we lose
        ordering guarantees.

        Waiting on a count of consumed messages can be unreliable: if we stop consuming when num_consumed == num_acked,
        we might exit early if some messages are duplicated (though not an issue here since producer retries==0)

        Therefore rely here on the consumer.timeout.ms setting which times out on the interval between successively
        consumed messages. Since we run the producer to completion before running the consumer, this is a reliable
        indicator that nothing is left to consume.

        """
        self.num_bounce = num_bounce

        # Produce in a background thread while driving broker failures
        self.logger.debug("Producing messages...")
        self.start_producers()

        self.logger.debug("Driving failures...")
        self.drive_failures(failure, node_type)
        time.sleep(5)  # Keep on producing for a few more seconds
        self.stop_producers()

        self.acked = [producer.acked for producer in self.producers]
        self.not_acked = [producer.not_acked for producer in self.producers]
        self.logger.info("num not acked on topic %s: %d" % (producer.topic, producer.num_not_acked))
        self.logger.info("num acked on topic %s:     %d" % (producer.topic, producer.num_acked))

        # Consume all messages
        self.logger.debug("Consuming messages...")
        self.messages_consumed = []
        for consumer in self.consumers:
            consumer.start()
            consumer.wait()
            self.messages_consumed.append(consumer.messages_consumed[1])
            self.logger.info("num consumed from topic %s:  %d" % (consumer.topic, len(self.messages_consumed[-1])))

        # Check produced vs consumed
        self.logger.debug("Validating...")
        success, msg = self.validate()

        if not success:
            self.mark_for_collect(self.producer)

        assert success, msg

    def fetch_broker_node(self, topic, partition, node_type):

        if node_type == LEADER:
            return self.kafka.leader(topic=topic, partition=partition)
        elif node_type == FOLLOWER:
            return self.follower(topic, partition)
        elif node_type == CONTROLLER:
            return self.kafka.controller()
        else:
            raise RuntimeError("Unsupported node type.")

    def start_producers(self):
        for producer in self.producers:
            producer.start()

        for producer in self.producers:
            if not wait_until(lambda: producer.num_acked > 5, timeout_sec=5):
                raise RuntimeError("Producer failed to start in a reasonable amount of time: %s" % str(producer))

    def stop_producers(self):
        for producer in self.producers:
            producer.stop()

    # def follower(self, topic, partition):
    #     """Get a node which is has a replica for the given topic/partition but which is not the leader
    #     Short-cut implementation - might be safer to actually query zookeeper.
    #     """
    #     leader = self.kafka.leader(topic, partition)
    #     leader_idx = self.kafka.idx(leader)
    #
    #     assert self.kafka.num_nodes > 1 and self.replication_factor == self.kafka.num_nodes
    #     follow_idx = (leader_idx + 1) % self.kafka.num_nodes
    #     return self.kafka.get_node(follow_idx)

    def drive_failures(self, failure, node_type):

        if failure in {SOFT_KILL, HARD_KILL, CLEAN_KILL}:
            self.kill(failure, node_type)
        elif failure in {SOFT_BOUNCE, HARD_BOUNCE, CLEAN_BOUNCE}:
            self.bounce(failure, node_type, self.num_bounce)
        else:
            raise RuntimeError("Invalid failure type")

    def bounce(self, failure, node_type, num_bounce):
        for i in range(num_bounce):
            node_to_signal = self.fetch_broker_node(self.topic_to_fail, 0, node_type)

            if failure == CLEAN_BOUNCE or failure == HARD_BOUNCE:
                self.kafka.restart_node(node_to_signal, wait_sec=5, clean_shutdown=(failure == CLEAN_BOUNCE))
            elif failure == SOFT_BOUNCE:
                self.kafka.signal_node(node_to_signal, "SIGSTOP")
                time.sleep(20)
                self.kafka.signal_node(node_to_signal, "SIGCONT")
            else:
                raise RuntimeError("Invalid failure type.")

            time.sleep(6)

    def kill(self, failure, node_type):
        node_to_signal = self.fetch_broker_node(self.topic_to_fail, 0, node_type)

        if failure == CLEAN_KILL or failure == HARD_KILL:
            self.kafka.stop_node(node_to_signal, clean_shutdown=(failure == CLEAN_KILL))
        elif failure == SOFT_KILL:
            self.kafka.signal_node(node_to_signal, "SIGSTOP")
        else:
            raise RuntimeError("Invalid failure type")

    def validate(self):
        """Check that produced messages were consumed."""

        success = True
        msg = ""
        for i in range(len(self.topic_names)):
            consumed = self.messages_consumed[i]
            acked = self.acked[i]
            topic = self.topic_names[i]

            if len(consumed) == 0:
                msg += "No messages consumed under topic $s" % topic
                success = False

            if len(set(consumed)) != len(consumed):
                # There are duplicates. This is ok, so report it but don't fail the test
                msg += "There are duplicate messages in the log\n"

            if not set(consumed).issuperset(set(acked)):
                # Every acked message must appear in the logs. I.e. consumed messages must be superset of acked messages.
                acked_minus_consumed = set(acked) - set(consumed)
                success = False
                msg += "At least one acked message did not appear in the consumed messages for topic %s. acked_minus_consumed: %s" % (topic, str(acked_minus_consumed))

        if not success:
            # Collect all the data logs if there was a failure
            self.mark_for_collect(self.kafka)

        return success, msg




