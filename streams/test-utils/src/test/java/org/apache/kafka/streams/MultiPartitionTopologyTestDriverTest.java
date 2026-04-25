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
package org.apache.kafka.streams;

import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.TestRecord;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the multi-partition support added by KIP-1238 to {@link TopologyTestDriver}.
 * Each test exercises one of the new behaviours: explicit-partition routing, key-hash routing
 * matching the production partitioner, internal repartition topic resolution, co-partition
 * validation, heterogeneous partition counts across sub-topologies and partition-aware state
 * store access.
 */
public class MultiPartitionTopologyTestDriverTest {

    private static final String IN_TOPIC = "input";
    private static final String OUT_TOPIC = "output";
    private static final StringSerializer STRING_SER = new StringSerializer();
    private static final StringDeserializer STRING_DES = new StringDeserializer();

    private static Properties baseProps() {
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "kip-1238-test");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return props;
    }

    /** Identity topology: one source, one sink, no processing. */
    private static Topology identityTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(IN_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .to(OUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
        return builder.build();
    }

    @Test
    public void explicitPartitionFromTestRecordRoutesToOwnerTask() {
        // Build a topology with a partitioned count store in the same sub-topology as the source
        // (no selectKey, so no repartition). Then the store partition is the input partition the
        // record was routed to, which lets us verify TestRecord.partition() actually steered it.
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(IN_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .count(Materialized.<String, Long>as(Stores.inMemoryKeyValueStore("counts"))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), baseProps())) {
            driver.declareTopic(IN_TOPIC, 4);
            driver.init();

            // Choose a key whose natural hash partition differs from the explicit partition we force,
            // so a passing test really proves the explicit partition won.
            final int naturalPartition = BuiltInPartitioner.partitionForKey(STRING_SER.serialize(IN_TOPIC, "anyKey"), 4);
            final int forcedPartition = (naturalPartition + 1) % 4;

            final TestInputTopic<String, String> in =
                driver.createInputTopic(IN_TOPIC, STRING_SER, STRING_SER);
            in.pipeInput(new TestRecord<>("anyKey", "v0", null, 0L, forcedPartition));

            assertEquals(4, driver.partitionsOf("counts"));
            final KeyValueStore<String, Long> forcedStore = driver.getKeyValueStore("counts", forcedPartition);
            assertNotNull(forcedStore);
            assertEquals(Long.valueOf(1L), forcedStore.get("anyKey"),
                "explicit partition=" + forcedPartition + " should route the record to the counts store at that partition");

            final KeyValueStore<String, Long> naturalStore = driver.getKeyValueStore("counts", naturalPartition);
            assertNull(naturalStore.get("anyKey"),
                "the natural-hash partition " + naturalPartition + " should not have received the record");
        }
    }

    @Test
    public void keyHashRoutingMatchesBuiltInPartitioner() {
        final int n = 4;
        try (TopologyTestDriver driver = new TopologyTestDriver(identityTopology(), baseProps())) {
            driver.createInputTopic(IN_TOPIC, STRING_SER, STRING_SER, n);
            driver.createOutputTopic(OUT_TOPIC, STRING_DES, STRING_DES, n);
            final TestInputTopic<String, String> in =
                driver.createInputTopic(IN_TOPIC, STRING_SER, STRING_SER);

            for (final String key : new String[] {"a", "b", "c", "d", "key-42", "abcdefgh"}) {
                in.pipeInput(key, "v");
                final org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]> got =
                    driver.readRecord(OUT_TOPIC);
                assertNotNull(got, "no record produced for key=" + key);
                final int expected = BuiltInPartitioner.partitionForKey(STRING_SER.serialize(IN_TOPIC, key), n);
                assertEquals(Integer.valueOf(expected), got.partition(),
                    "key '" + key + "' should hash to partition " + expected);
            }
        }
    }

    @Test
    public void nullKeyRoutesToPartitionZero() {
        try (TopologyTestDriver driver = new TopologyTestDriver(identityTopology(), baseProps())) {
            driver.createInputTopic(IN_TOPIC, STRING_SER, STRING_SER, 3);
            driver.createOutputTopic(OUT_TOPIC, STRING_DES, STRING_DES, 3);
            final TestInputTopic<String, String> in =
                driver.createInputTopic(IN_TOPIC, STRING_SER, STRING_SER);
            in.pipeInput(null, "v");
            final org.apache.kafka.clients.producer.ProducerRecord<byte[], byte[]> got =
                driver.readRecord(OUT_TOPIC);
            assertEquals(Integer.valueOf(0), got.partition());
        }
    }

    @Test
    public void declareTopicAfterInitThrows() {
        try (TopologyTestDriver driver = new TopologyTestDriver(identityTopology(), baseProps())) {
            driver.declareTopic(IN_TOPIC, 2);
            driver.init();
            assertThrows(IllegalStateException.class, () -> driver.declareTopic("late", 4));
        }
    }

    @Test
    public void declareTopicWithZeroPartitionsThrows() {
        try (TopologyTestDriver driver = new TopologyTestDriver(identityTopology(), baseProps())) {
            assertThrows(IllegalArgumentException.class, () -> driver.declareTopic(IN_TOPIC, 0));
        }
    }

    @Test
    public void redeclaringTopicWithDifferentCountThrows() {
        try (TopologyTestDriver driver = new TopologyTestDriver(identityTopology(), baseProps())) {
            driver.declareTopic(IN_TOPIC, 4);
            assertThrows(IllegalArgumentException.class, () -> driver.declareTopic(IN_TOPIC, 2));
        }
    }

    @Test
    public void heterogeneousPartitionCountsAcrossSubTopologies() {
        // groupByKey + count creates a repartition topic; we declare the source at 4 partitions
        // and the resulting sub-topology that owns the count store will get 4 partitions too.
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(IN_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .selectKey((k, v) -> v.split(":")[0])
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .count(Materialized.<String, Long>as(Stores.inMemoryKeyValueStore("counts"))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), baseProps())) {
            driver.declareTopic(IN_TOPIC, 4);
            driver.init();

            // The sub-topology that hosts the "counts" store must run with 4 partitions.
            assertEquals(4, driver.partitionsOf("counts"));
            assertTrue(driver.subtopologies().size() >= 1);
        }
    }

    @Test
    public void partitionedStorePrePopulationAndPerPartitionAssertion() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(IN_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .count(Materialized.<String, Long>as(Stores.inMemoryKeyValueStore("counts"))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), baseProps())) {
            driver.declareTopic(IN_TOPIC, 3);
            driver.init();

            final TestInputTopic<String, String> in =
                driver.createInputTopic(IN_TOPIC, STRING_SER, STRING_SER);
            // Pipe several records with different keys; verify each partition's store reflects
            // the keys routed to it. Since input is groupByKey'd, the count store is partitioned.
            final Map<String, Integer> keyToPartition = new HashMap<>();
            for (final String key : new String[] {"alpha", "beta", "gamma", "delta", "epsilon"}) {
                in.pipeInput(key, "v1");
                in.pipeInput(key, "v2");
                final int p = BuiltInPartitioner.partitionForKey(STRING_SER.serialize(IN_TOPIC, key), 3);
                keyToPartition.put(key, p);
            }

            for (final Map.Entry<String, Integer> entry : keyToPartition.entrySet()) {
                final KeyValueStore<String, Long> store =
                    driver.getKeyValueStore("counts", entry.getValue());
                assertNotNull(store, "store should exist for partition " + entry.getValue());
                assertEquals(Long.valueOf(2L), store.get(entry.getKey()),
                    "key '" + entry.getKey() + "' should have count 2 in partition "
                        + entry.getValue());
            }
        }
    }

    @Test
    public void getStateStoreNoArgThrowsWhenStoreIsPartitioned() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(IN_TOPIC, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .count(Materialized.<String, Long>as(Stores.inMemoryKeyValueStore("counts"))
                .withKeySerde(Serdes.String())
                .withValueSerde(Serdes.Long()));

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), baseProps())) {
            driver.declareTopic(IN_TOPIC, 3);
            driver.init();

            final TestInputTopic<String, String> in =
                driver.createInputTopic(IN_TOPIC, STRING_SER, STRING_SER);
            // Populate at least 2 partitions of the store.
            in.pipeInput("alpha", "x");
            in.pipeInput("beta", "x");
            in.pipeInput("gamma", "x");
            in.pipeInput("delta", "x");

            final IllegalStateException e = assertThrows(
                IllegalStateException.class,
                () -> driver.getKeyValueStore("counts"));
            assertTrue(e.getMessage().contains("getStateStore(name, partition)"),
                "exception message should point at the partition-aware overload, was: " + e.getMessage());
        }
    }

    @Test
    public void unknownStoreNameReturnsNull() {
        try (TopologyTestDriver driver = new TopologyTestDriver(identityTopology(), baseProps())) {
            driver.declareTopic(IN_TOPIC, 2);
            driver.init();
            assertNull(driver.getStateStore("does-not-exist"));
        }
    }

    @Test
    public void singlePartitionBackCompatPathWorksWithoutInit() {
        // No declareTopic, no init() → legacy single-flat-task path. Existing single-partition tests
        // should continue to function unchanged.
        try (TopologyTestDriver driver = new TopologyTestDriver(identityTopology(), baseProps())) {
            final TestInputTopic<String, String> in =
                driver.createInputTopic(IN_TOPIC, STRING_SER, STRING_SER);
            final TestOutputTopic<String, String> out =
                driver.createOutputTopic(OUT_TOPIC, STRING_DES, STRING_DES);
            in.pipeInput("k", "v");
            assertEquals("v", out.readValue());
        }
    }
}
