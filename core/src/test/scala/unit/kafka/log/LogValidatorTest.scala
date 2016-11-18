/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log

import java.nio.ByteBuffer

import kafka.common.LongRef
import kafka.message._
import org.apache.kafka.common.errors.InvalidTimestampException
import org.apache.kafka.common.record._
import org.junit.Assert._
import org.junit.Test
import org.scalatest.junit.JUnitSuite

import scala.collection.JavaConverters._

class LogValidatorTest extends JUnitSuite {

  @Test
  def testLogAppendTimeNonCompressedV1() {
    val now = System.currentTimeMillis()
    // The timestamps should be overwritten
    val logBuffer = createLogBuffer(magicValue = Record.MAGIC_VALUE_V1, timestamp = 0L, codec = CompressionType.NONE)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(logBuffer,
      offsetCounter = new LongRef(0),
      now = now,
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedLogBuffer = validatedResults.validatedEntries
    assertEquals("message set size should not change", logBuffer.records.asScala.size, validatedLogBuffer.records.asScala.size)
    validatedLogBuffer.shallowIterator.asScala.foreach(logEntry => validateLogAppendTime(now, logEntry))
    assertEquals(s"Max timestamp should be $now", now, validatedResults.maxTimestamp)
    assertEquals(s"The offset of max timestamp should be 0", 0, validatedResults.offsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatedResults.messageSizeMaybeChanged)
  }

  @Test
  def testLogAppendTimeNonCompressedV2() {
    val now = System.currentTimeMillis()
    // The timestamps should be overwritten
    val logBuffer = createLogBuffer(magicValue = Record.MAGIC_VALUE_V2, timestamp = 0L, codec = CompressionType.NONE)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(logBuffer,
      offsetCounter = new LongRef(0),
      now = now,
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V2,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedLogBuffer = validatedResults.validatedEntries
    assertEquals("message set size should not change", logBuffer.records.asScala.size, validatedLogBuffer.records.asScala.size)
    validatedLogBuffer.shallowIterator.asScala.foreach(logEntry => validateLogAppendTime(now, logEntry))
    assertEquals(s"Max timestamp should be $now", now, validatedResults.maxTimestamp)
    assertEquals("The offset of max timestamp should be 0", 0, validatedResults.offsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatedResults.messageSizeMaybeChanged)
  }

  @Test
  def testLogAppendTimeWithRecompressionV1() {
    val now = System.currentTimeMillis()
    // The timestamps should be overwritten
    val logBuffer = createLogBuffer(magicValue = Record.MAGIC_VALUE_V0, codec = CompressionType.GZIP)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(
      logBuffer,
      offsetCounter = new LongRef(0),
      now = now,
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedLogBuffer = validatedResults.validatedEntries

    assertEquals("message set size should not change", logBuffer.records.asScala.size, validatedLogBuffer.records.asScala.size)
    validatedLogBuffer.shallowIterator.asScala.foreach(logEntry => validateLogAppendTime(now, logEntry))
    assertTrue("MessageSet should still valid", validatedLogBuffer.shallowIterator.next().isValid)
    assertEquals(s"Max timestamp should be $now", now, validatedResults.maxTimestamp)
    assertEquals(s"The offset of max timestamp should be ${logBuffer.records.asScala.size - 1}",
      logBuffer.records.asScala.size - 1, validatedResults.offsetOfMaxTimestamp)
    assertTrue("Message size may have been changed", validatedResults.messageSizeMaybeChanged)
  }

  @Test
  def testLogAppendTimeWithRecompressionV2() {
    val now = System.currentTimeMillis()
    // The timestamps should be overwritten
    val logBuffer = createLogBuffer(magicValue = Record.MAGIC_VALUE_V0, codec = CompressionType.GZIP)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(
      logBuffer,
      offsetCounter = new LongRef(0),
      now = now,
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V2,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedLogBuffer = validatedResults.validatedEntries

    assertEquals("message set size should not change", logBuffer.records.asScala.size, validatedLogBuffer.records.asScala.size)
    validatedLogBuffer.shallowIterator.asScala.foreach(logEntry => validateLogAppendTime(now, logEntry))
    assertTrue("MessageSet should still valid", validatedLogBuffer.shallowIterator.next().isValid)
    assertEquals(s"Max timestamp should be $now", now, validatedResults.maxTimestamp)
    assertEquals(s"The offset of max timestamp should be ${logBuffer.records.asScala.size - 1}",
      logBuffer.records.asScala.size - 1, validatedResults.offsetOfMaxTimestamp)
    assertTrue("Message size may have been changed", validatedResults.messageSizeMaybeChanged)
  }

  @Test
  def testLogAppendTimeWithoutRecompression() {
    val now = System.currentTimeMillis()
    // The timestamps should be overwritten
    val logBuffer = createLogBuffer(magicValue = Record.MAGIC_VALUE_V1,
      timestamp = 0L, codec = CompressionType.GZIP)
    val validatedResults = LogValidator.validateMessagesAndAssignOffsets(
      logBuffer,
      offsetCounter = new LongRef(0),
      now = now,
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedLogBuffer = validatedResults.validatedEntries

    assertEquals("message set size should not change", logBuffer.records.asScala.size,
      validatedLogBuffer.records.asScala.size)
    validatedLogBuffer.shallowIterator.asScala.foreach(logEntry => validateLogAppendTime(now, logEntry))
    assertTrue("MessageSet should still valid", validatedLogBuffer.shallowIterator.next().isValid)
    assertEquals(s"Max timestamp should be $now", now, validatedResults.maxTimestamp)
    assertEquals(s"The offset of max timestamp should be ${logBuffer.records.asScala.size - 1}",
      logBuffer.records.asScala.size - 1, validatedResults.offsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatedResults.messageSizeMaybeChanged)
  }

  @Test
  def testCreateTimeNonCompressedV1() {
    val now = System.currentTimeMillis()
    val timestampSeq = Seq(now - 1, now + 1, now)
    val logBuffer =
      MemoryLogBuffer.withRecords(CompressionType.NONE,
        Record.create(Record.MAGIC_VALUE_V1, timestampSeq(0), "hello".getBytes),
        Record.create(Record.MAGIC_VALUE_V1, timestampSeq(1), "there".getBytes),
        Record.create(Record.MAGIC_VALUE_V1, timestampSeq(2), "beautiful".getBytes))

    val validatingResults = LogValidator.validateMessagesAndAssignOffsets(logBuffer,
      offsetCounter = new LongRef(0),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedLogBuffer = validatingResults.validatedEntries

    var i = 0
    for (entry <- validatedLogBuffer.shallowIterator.asScala) {
      assertTrue(entry.isValid)
      assertEquals(entry.timestampType, TimestampType.CREATE_TIME)
      assertEquals(entry.timestamp, entry.asScala.map(_.timestamp).max)
      for (record <- entry.asScala) {
        assertTrue(record.isValid)
        assertEquals(record.timestamp, timestampSeq(i))
        i += 1
      }
    }
    assertEquals(s"Max timestamp should be ${now + 1}", now + 1, validatingResults.maxTimestamp)
    assertEquals(s"Offset of max timestamp should be 1", 1, validatingResults.offsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatingResults.messageSizeMaybeChanged)
  }

  @Test
  def testCreateTimeNonCompressedV2() {
    val now = System.currentTimeMillis()
    val timestampSeq = Seq(now - 1, now + 1, now)
    val buf = ByteBuffer.allocate(512)
    val builder = MemoryLogBuffer.builder(buf, Record.MAGIC_VALUE_V2, CompressionType.NONE, TimestampType.CREATE_TIME)
    builder.append(0, timestampSeq(0), null, "hello".getBytes)
    builder.append(1, timestampSeq(1), null, "there".getBytes)
    builder.append(2, timestampSeq(2), null, "beautiful".getBytes)
    val logBuffer = builder.build()

    val validatingResults = LogValidator.validateMessagesAndAssignOffsets(logBuffer,
      offsetCounter = new LongRef(0),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V2,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L)
    val validatedLogBuffer = validatingResults.validatedEntries

    var i = 0
    for (entry <- validatedLogBuffer.shallowIterator.asScala) {
      assertTrue(entry.isValid)
      assertEquals(entry.timestampType, TimestampType.CREATE_TIME)
      assertEquals(entry.timestamp, entry.asScala.map(_.timestamp).max)
      for (record <- entry.asScala) {
        assertTrue(record.isValid)
        assertEquals(record.timestamp, timestampSeq(i))
        i += 1
      }
    }
    assertEquals(s"Max timestamp should be ${now + 1}", now + 1, validatingResults.maxTimestamp)
    assertEquals(s"Offset of max timestamp should be 1", 1, validatingResults.offsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatingResults.messageSizeMaybeChanged)
  }

  @Test
  def testCreateTimeCompressedV1() {
    val now = System.currentTimeMillis()
    val timestampSeq = Seq(now - 1, now + 1, now)
    val logBuffer =
      MemoryLogBuffer.withRecords(CompressionType.GZIP,
        Record.create(Record.MAGIC_VALUE_V1, timestampSeq(0), "hello".getBytes),
        Record.create(Record.MAGIC_VALUE_V1, timestampSeq(1), "there".getBytes),
        Record.create(Record.MAGIC_VALUE_V1, timestampSeq(2), "beautiful".getBytes))

    val validatedResults =
      LogValidator.validateMessagesAndAssignOffsets(logBuffer,
        offsetCounter = new LongRef(0),
        now = System.currentTimeMillis(),
        sourceCodec = DefaultCompressionCodec,
        targetCodec = DefaultCompressionCodec,
        messageFormatVersion = Record.MAGIC_VALUE_V1,
        messageTimestampType = TimestampType.CREATE_TIME,
        messageTimestampDiffMaxMs = 1000L)
    val validatedLogBuffer = validatedResults.validatedEntries

    var i = 0
    for (entry <- validatedLogBuffer.shallowIterator.asScala) {
      assertTrue(entry.isValid)
      assertEquals(entry.timestampType, TimestampType.CREATE_TIME)
      assertEquals(entry.timestamp, entry.asScala.map(_.timestamp).max)
      for (record <- entry.asScala) {
        assertTrue(record.isValid)
        assertEquals(record.timestamp, timestampSeq(i))
        i += 1
      }
    }
    assertEquals(s"Max timestamp should be ${now + 1}", now + 1, validatedResults.maxTimestamp)
    assertEquals(s"Offset of max timestamp should be ${validatedLogBuffer.records.asScala.size - 1}",
      validatedLogBuffer.records.asScala.size - 1, validatedResults.offsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatedResults.messageSizeMaybeChanged)
  }

  @Test
  def testCreateTimeCompressedV2() {
    val now = System.currentTimeMillis()
    val timestampSeq = Seq(now - 1, now + 1, now)
    val buf = ByteBuffer.allocate(512)
    val builder = MemoryLogBuffer.builder(buf, Record.MAGIC_VALUE_V2, CompressionType.NONE, TimestampType.CREATE_TIME)
    builder.append(0, timestampSeq(0), null, "hello".getBytes)
    builder.append(1, timestampSeq(1), null, "there".getBytes)
    builder.append(2, timestampSeq(2), null, "beautiful".getBytes)
    val logBuffer = builder.build()

    val validatedResults =
      LogValidator.validateMessagesAndAssignOffsets(logBuffer,
        offsetCounter = new LongRef(0),
        now = System.currentTimeMillis(),
        sourceCodec = DefaultCompressionCodec,
        targetCodec = DefaultCompressionCodec,
        messageFormatVersion = Record.MAGIC_VALUE_V2,
        messageTimestampType = TimestampType.CREATE_TIME,
        messageTimestampDiffMaxMs = 1000L)
    val validatedLogBuffer = validatedResults.validatedEntries

    var i = 0
    for (entry <- validatedLogBuffer.shallowIterator.asScala) {
      assertTrue(entry.isValid)
      assertEquals(entry.timestampType, TimestampType.CREATE_TIME)
      assertEquals(entry.timestamp, entry.asScala.map(_.timestamp).max)
      for (record <- entry.asScala) {
        assertTrue(record.isValid)
        assertEquals(record.timestamp, timestampSeq(i))
        i += 1
      }
    }
    assertEquals(s"Max timestamp should be ${now + 1}", now + 1, validatedResults.maxTimestamp)
    assertEquals(s"Offset of max timestamp should be ${validatedLogBuffer.records.asScala.size - 1}",
      validatedLogBuffer.records.asScala.size - 1, validatedResults.offsetOfMaxTimestamp)
    assertFalse("Message size should not have been changed", validatedResults.messageSizeMaybeChanged)
  }

  @Test(expected = classOf[InvalidTimestampException])
  def testInvalidCreateTimeNonCompressed() {
    val now = System.currentTimeMillis()
    val logBuffer = createLogBuffer(magicValue = Record.MAGIC_VALUE_V1, timestamp = now - 1001L,
      codec = CompressionType.NONE)
    LogValidator.validateMessagesAndAssignOffsets(
      logBuffer,
      offsetCounter = new LongRef(0),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L)
  }

  @Test(expected = classOf[InvalidTimestampException])
  def testInvalidCreateTimeCompressed() {
    val now = System.currentTimeMillis()
    val logBuffer = createLogBuffer(magicValue = Record.MAGIC_VALUE_V1, timestamp = now - 1001L,
      codec = CompressionType.GZIP)
    LogValidator.validateMessagesAndAssignOffsets(
      logBuffer,
      offsetCounter = new LongRef(0),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L)
  }
  @Test
  def testAbsoluteOffsetAssignmentNonCompressed() {
    val logBuffer = createLogBuffer(magicValue = Record.MAGIC_VALUE_V0, codec = CompressionType.NONE)
    val offset = 1234567
    checkOffsets(logBuffer, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(logBuffer,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V0,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L).validatedEntries, offset)
  }

  @Test
  def testAbsoluteOffsetAssignmentCompressed() {
    val logBuffer = createLogBuffer(magicValue = Record.MAGIC_VALUE_V0, codec = CompressionType.GZIP)
    val offset = 1234567
    checkOffsets(logBuffer, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(logBuffer,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V0,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 1000L).validatedEntries, offset)
  }

  @Test
  def testRelativeOffsetAssignmentNonCompressedV1() {
    val now = System.currentTimeMillis()
    val logBuffer = createLogBuffer(magicValue = Record.MAGIC_VALUE_V1, timestamp = now, codec = CompressionType.NONE)
    val offset = 1234567
    checkOffsets(logBuffer, 0)
    val messageWithOffset = LogValidator.validateMessagesAndAssignOffsets(logBuffer,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedEntries
    checkOffsets(messageWithOffset, offset)
  }

  @Test
  def testRelativeOffsetAssignmentNonCompressedV2() {
    val now = System.currentTimeMillis()
    val logBuffer = createLogBuffer(magicValue = Record.MAGIC_VALUE_V2, timestamp = now, codec = CompressionType.NONE)
    val offset = 1234567
    checkOffsets(logBuffer, 0)
    val messageWithOffset = LogValidator.validateMessagesAndAssignOffsets(logBuffer,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V2,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedEntries
    checkOffsets(messageWithOffset, offset)
  }

  @Test
  def testRelativeOffsetAssignmentCompressedV1() {
    val now = System.currentTimeMillis()
    val logBuffer = createLogBuffer(magicValue = Record.MAGIC_VALUE_V1, timestamp = now, codec = CompressionType.GZIP)
    val offset = 1234567
    checkOffsets(logBuffer, 0)
    val compressedMessagesWithOffset = LogValidator.validateMessagesAndAssignOffsets(
      logBuffer,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedEntries
    checkOffsets(compressedMessagesWithOffset, offset)
  }

  @Test
  def testRelativeOffsetAssignmentCompressedV2() {
    val now = System.currentTimeMillis()
    val logBuffer = createLogBuffer(magicValue = Record.MAGIC_VALUE_V2, timestamp = now, codec = CompressionType.GZIP)
    val offset = 1234567
    checkOffsets(logBuffer, 0)
    val compressedMessagesWithOffset = LogValidator.validateMessagesAndAssignOffsets(
      logBuffer,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V2,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedEntries
    checkOffsets(compressedMessagesWithOffset, offset)
  }

  @Test
  def testOffsetAssignmentAfterMessageFormatConversionV0NonCompressed() {
    val logBuffer = createLogBuffer(magicValue = Record.MAGIC_VALUE_V0, codec = CompressionType.NONE)
    checkOffsets(logBuffer, 0)
    val offset = 1234567
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(logBuffer,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L).validatedEntries, offset)
  }

  @Test
  def testOffsetAssignmentAfterMessageFormatConversionV0Compressed() {
    val logBuffer = createLogBuffer(magicValue = Record.MAGIC_VALUE_V0, codec = CompressionType.GZIP)
    val offset = 1234567
    checkOffsets(logBuffer, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(logBuffer,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.LOG_APPEND_TIME,
      messageTimestampDiffMaxMs = 1000L).validatedEntries, offset)
  }

  @Test
  def testOffsetAssignmentAfterMessageFormatConversionV1NonCompressed() {
    val offset = 1234567
    val now = System.currentTimeMillis()
    val logBuffer = createLogBuffer(Record.MAGIC_VALUE_V1, now, codec = CompressionType.NONE)
    checkOffsets(logBuffer, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(logBuffer,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = NoCompressionCodec,
      targetCodec = NoCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V0,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedEntries, offset)
  }

  @Test
  def testOffsetAssignmentAfterMessageFormatConversionV1Compressed() {
    val offset = 1234567
    val now = System.currentTimeMillis()
    val logBuffer = createLogBuffer(Record.MAGIC_VALUE_V1, now, CompressionType.GZIP)
    checkOffsets(logBuffer, 0)
    checkOffsets(LogValidator.validateMessagesAndAssignOffsets(logBuffer,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = DefaultCompressionCodec,
      targetCodec = DefaultCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V0,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L).validatedEntries, offset)
  }

  @Test(expected = classOf[InvalidRecordException])
  def testInvalidInnerMagicVersion(): Unit = {
    val offset = 1234567
    val logBuffer = logBufferWithInvalidInnerMagic(offset)
    LogValidator.validateMessagesAndAssignOffsets(logBuffer,
      offsetCounter = new LongRef(offset),
      now = System.currentTimeMillis(),
      sourceCodec = SnappyCompressionCodec,
      targetCodec = SnappyCompressionCodec,
      messageFormatVersion = Record.MAGIC_VALUE_V1,
      messageTimestampType = TimestampType.CREATE_TIME,
      messageTimestampDiffMaxMs = 5000L)
  }

  private def createLogBuffer(magicValue: Byte = Message.CurrentMagicValue,
                              timestamp: Long = Message.NoTimestamp,
                              codec: CompressionType = CompressionType.NONE): MemoryLogBuffer = {
    val buf = ByteBuffer.allocate(512)
    val builder = MemoryLogBuffer.builder(buf, magicValue, codec, TimestampType.CREATE_TIME)
    builder.append(0, timestamp, null, "hello".getBytes)
    builder.append(1, timestamp, null, "there".getBytes)
    builder.append(2, timestamp, null, "beautiful".getBytes)
    builder.build()
  }

  /* check that offsets are assigned consecutively from the given base offset */
  def checkOffsets(logBuffer: MemoryLogBuffer, baseOffset: Long) {
    assertTrue("Message set should not be empty", logBuffer.records.asScala.nonEmpty)
    var offset = baseOffset
    for (entry <- logBuffer.records.asScala) {
      assertEquals("Unexpected offset in message set iterator", offset, entry.offset)
      offset += 1
    }
  }

  private def logBufferWithInvalidInnerMagic(initialOffset: Long): MemoryLogBuffer = {
    val records = (0 until 20).map(id =>
      Record.create(Record.MAGIC_VALUE_V0,
        Record.NO_TIMESTAMP,
        id.toString.getBytes,
        id.toString.getBytes))

    val buffer = ByteBuffer.allocate(math.min(math.max(records.map(_.size()).sum / 2, 1024), 1 << 16))
    val builder = MemoryLogBuffer.builder(buffer, Record.MAGIC_VALUE_V1, CompressionType.GZIP,
      TimestampType.CREATE_TIME)

    var offset = initialOffset
    records.foreach { record =>
      builder.appendUnchecked(offset, record)
      offset += 1
    }

    builder.build()
  }

  def validateLogAppendTime(now: Long, entry: LogEntry) {
    assertTrue(entry.isValid)
    assertTrue(entry.timestampType() == TimestampType.LOG_APPEND_TIME)
    assertEquals(s"Timestamp of message $entry should be $now", now, entry.timestamp)
    for (record <- entry.asScala) {
      assertTrue(record.isValid)
      assertEquals(s"Timestamp of message $record should be $now", now, record.timestamp)
    }
  }

}
