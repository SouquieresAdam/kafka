/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.test;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;

import java.time.Instant;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * A key/value pair, including timestamp and record headers, to be sent to or received from {@link TopologyTestDriver}.
 * If [a] record does not contain a timestamp,
 * {@link TestInputTopic} will auto advance it's time when the record is piped.
 */
public class TestRecord<K, V> {
    /**
     * Sentinel returned by {@link #partition()} when no explicit partition was set on the record.
     * A record carrying this value is routed by the driver using the record key's hash.
     */
    private static final int NO_PARTITION = -1;

    private final Headers headers;
    private final K key;
    private final V value;
    private final Instant recordTime;
    private final int partition;

    /**
     * Creates a record.
     *
     * @param key The key that will be included in the record
     * @param value The value of the record
     * @param headers the record headers that will be included in the record
     * @param recordTime The timestamp of the record.
     */
    public TestRecord(final K key, final V value, final Headers headers, final Instant recordTime) {
        this(key, value, headers, recordTime, NO_PARTITION);
    }

    /**
     * Creates a record with an explicit target partition.
     *
     * @param key The key that will be included in the record
     * @param value The value of the record
     * @param headers the record headers that will be included in the record
     * @param recordTime The timestamp of the record.
     * @param partition The target partition for this record, or a negative value to let the driver route by key hash.
     */
    public TestRecord(final K key, final V value, final Headers headers, final Instant recordTime, final int partition) {
        this.key = key;
        this.value = value;
        this.recordTime = recordTime;
        this.headers = new RecordHeaders(headers);
        this.partition = partition;
    }

    /**
     * Creates a record.
     *
     * @param key The key that will be included in the record
     * @param value The value of the record
     * @param headers the record headers that will be included in the record
     * @param timestampMs The timestamp of the record, in milliseconds since the beginning of the epoch.
     */
    public TestRecord(final K key, final V value, final Headers headers, final Long timestampMs) {
        if (timestampMs != null) {
            if (timestampMs < 0) {
                throw new IllegalArgumentException(
                    String.format("Invalid timestamp: %d. Timestamp should always be non-negative or null.", timestampMs));
            }
            this.recordTime = Instant.ofEpochMilli(timestampMs);
        } else {
            this.recordTime = null;
        }
        this.key = key;
        this.value = value;
        this.headers = new RecordHeaders(headers);
        this.partition = NO_PARTITION;
    }

    /**
     * Creates a record.
     *
     * @param key The key of the record
     * @param value The value of the record
     * @param recordTime The timestamp of the record as Instant.
     */
    public TestRecord(final K key, final V value, final Instant recordTime) {
        this(key, value, null, recordTime);
    }

    /**
     * Creates a record.
     *
     * @param key The key of the record
     * @param value The value of the record
     * @param headers The record headers that will be included in the record
     */
    public TestRecord(final K key, final V value, final Headers headers) {
        this.key = key;
        this.value = value;
        this.headers = new RecordHeaders(headers);
        this.recordTime = null;
        this.partition = NO_PARTITION;
    }

    /**
     * Creates a record.
     *
     * @param key The key of the record
     * @param value The value of the record
     */
    public TestRecord(final K key, final V value) {
        this.key = key;
        this.value = value;
        this.headers = new RecordHeaders();
        this.recordTime = null;
        this.partition = NO_PARTITION;
    }

    /**
     * Create a record with {@code null} key.
     *
     * @param value The value of the record
     */
    public TestRecord(final V value) {
        this(null, value);
    }

    /**
     * Create a {@code TestRecord} from a {@link ConsumerRecord}.
     *
     * @param record The v
     */
    public TestRecord(final ConsumerRecord<K, V> record) {
        Objects.requireNonNull(record);
        this.key = record.key();
        this.value = record.value();
        this.headers = record.headers();
        this.recordTime = Instant.ofEpochMilli(record.timestamp());
        this.partition = NO_PARTITION;
    }

    /**
     * Create a {@code TestRecord} from a {@link ProducerRecord}.
     *
     * @param record The record contents
     */
    public TestRecord(final ProducerRecord<K, V> record) {
        Objects.requireNonNull(record);
        this.key = record.key();
        this.value = record.value();
        this.headers = record.headers();
        this.recordTime = Instant.ofEpochMilli(record.timestamp());
        this.partition = NO_PARTITION;
    }

    /**
     * @return The headers.
     */
    public Headers headers() {
        return headers;
    }

    /**
     * @return The key (or {@code null} if no key is specified).
     */
    public K key() {
        return key;
    }

    /**
     * @return The value.
     */
    public V value() {
        return value;
    }

    /**
     * @return The timestamp, which is in milliseconds since epoch.
     */
    public Long timestamp() {
        return this.recordTime == null ? null : this.recordTime.toEpochMilli();
    }

    /**
     * @return The headers.
     */
    public Headers getHeaders() {
        return headers;
    }

    /**
     * @return The key (or null if no key is specified)
     */
    public K getKey() {
        return key;
    }

    /**
     * @return The value.
     */
    public V getValue() {
        return value;
    }

    /**
     * @return The timestamp.
     */
    public Instant getRecordTime() {
        return recordTime;
    }

    /**
     * @return The explicit target partition, or {@code -1} if the record should be routed by key hash.
     */
    public int partition() {
        return partition;
    }

    @Override
    public String toString() {
        final StringJoiner joiner = new StringJoiner(", ", TestRecord.class.getSimpleName() + "[", "]")
                .add("key=" + key)
                .add("value=" + value)
                .add("headers=" + headers)
                .add("recordTime=" + recordTime);
        if (partition != NO_PARTITION) {
            joiner.add("partition=" + partition);
        }
        return joiner.toString();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final TestRecord<?, ?> that = (TestRecord<?, ?>) o;
        return partition == that.partition && fieldsEqualIgnoringPartition(that);
    }

    /**
     * Compares this record to another for equality on every field <em>except</em> the partition.
     * Useful when asserting on driver output where the routed partition is irrelevant to the test.
     *
     * @param that the record to compare with
     * @return {@code true} if {@code that} is equal to this record ignoring the partition
     */
    public boolean equalsIgnorePartition(final TestRecord<K, V> that) {
        return that != null && fieldsEqualIgnoringPartition(that);
    }

    private boolean fieldsEqualIgnoringPartition(final TestRecord<?, ?> that) {
        return Objects.equals(headers, that.headers) &&
            Objects.equals(key, that.key) &&
            Objects.equals(value, that.value) &&
            Objects.equals(recordTime, that.recordTime);
    }

    @Override
    public int hashCode() {
        return Objects.hash(headers, key, value, recordTime, partition);
    }
}
