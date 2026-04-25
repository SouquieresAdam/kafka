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
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.TestRecord;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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

    /**
     * Exercises four KIP-1238 concerns at once with a single topology:
     * <ul>
     *   <li><b>Two sources with different partition counts:</b> {@code inA} at 4 partitions
     *       feeds sub-topology A; {@code inB} at 2 partitions feeds sub-topology B.</li>
     *   <li><b>Two sinks with different partition counts:</b> {@code outA} at 4 partitions
     *       (preserves source A's partitioning); {@code outB} at 3 partitions (after the
     *       explicit repartition).</li>
     *   <li><b>An explicit per-link repartition count:</b> the path from {@code inB} goes
     *       through {@link Repartitioned#withNumberOfPartitions(int)
     *       Repartitioned.withNumberOfPartitions(3)}, so the repartition topic and the
     *       downstream sub-topology are pinned at 3 partitions, independent of the
     *       2-partition source.</li>
     *   <li><b>Two processors sharing the same state store:</b> on sub-topology A, two
     *       chained PAPI {@link Processor}s both reference the {@code shared} key-value store
     *       and read/write the same partition-local instance. After one input record on
     *       {@code inA}, the store at the source partition holds 2 (each processor
     *       incremented once).</li>
     * </ul>
     * Sub-topologies A and B are deliberately disjoint: a state store cannot be shared
     * across sub-topologies (only globals can), and putting both sources in the same
     * sub-topology would force them to be co-partitioned, defeating the
     * "different input partition counts" goal.
     */
    @Test
    public void twoSourcesTwoSinksDifferentPartitionsAndExplicitRepartitionAndSharedStore() {
        final String inA = "inA";    // source A: 4 partitions
        final String inB = "inB";    // source B: 2 partitions
        final String outA = "outA";  // sink A: 4 partitions (preserves source A)
        final String outB = "outB";  // sink B: 3 partitions (after explicit repartition)

        final StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("shared"),
            Serdes.String(),
            Serdes.Long()));

        // Sub-topology A: inA fans out to TWO PAPI processors in parallel; both reference the same
        // "shared" store name, so they share the partition-local store instance. The first
        // processor's output is intentionally discarded — only its store side-effect matters; the
        // second processor's output goes to outA.
        final KStream<String, String> streamA =
            builder.stream(inA, Consumed.with(Serdes.String(), Serdes.String()));
        streamA.process(SharedStoreCounter::new, "shared");
        streamA
            .process(SharedStoreCounter::new, "shared")
            .to(outA, Produced.with(Serdes.String(), Serdes.Long()));

        // Sub-topology B: inB → explicit repartition(3) → outB. No state store; just verifies
        // the explicit partition count propagates from the Repartitioned config to the sink.
        final KStream<String, String> streamB =
            builder.stream(inB, Consumed.with(Serdes.String(), Serdes.String()));
        streamB
            .repartition(Repartitioned.<String, String>with(Serdes.String(), Serdes.String())
                .withNumberOfPartitions(3))
            .to(outB, Produced.with(Serdes.String(), Serdes.String()));

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), baseProps())) {
            driver.declareTopic(inA, 4);
            driver.declareTopic(inB, 2);
            driver.declareTopic(outA, 4);
            driver.declareTopic(outB, 3);
            driver.init();

            // Pipe one record on inA. procA1 reads store=null, writes 1, forwards (k, 1L).
            // procA2 reads store=1, writes 2, forwards (k, 2L). Final store at source partition = 2.
            final TestInputTopic<String, String> pipeA =
                driver.createInputTopic(inA, STRING_SER, STRING_SER);
            pipeA.pipeInput("k", "v");

            final int srcPartA = BuiltInPartitioner.partitionForKey(STRING_SER.serialize(inA, "k"), 4);
            final KeyValueStore<String, Long> sharedStore =
                driver.getKeyValueStore("shared", srcPartA);
            assertNotNull(sharedStore, "shared store should exist at source partition " + srcPartA);
            assertEquals(Long.valueOf(2L), sharedStore.get("k"),
                "both PAPI processors share the same store instance and each incremented once");

            // Pipe one record on inB. It traverses the explicit repartition(3) and lands on outB.
            final TestInputTopic<String, String> pipeB =
                driver.createInputTopic(inB, STRING_SER, STRING_SER);
            pipeB.pipeInput("k2", "vb");

            // Verify the partition counts of each sub-topology. Sub-topology A inherits 4 from
            // inA; sub-topology B's downstream side is pinned at 3 by Repartitioned.
            int sidA = -1;
            int sidB = -1;
            for (final int sid : driver.subtopologies()) {
                final int n = driver.partitionsOfSubtopology(sid);
                if (n == 4) {
                    sidA = sid;
                } else if (n == 3) {
                    sidB = sid;
                }
            }
            assertTrue(sidA >= 0, "expected one sub-topology with 4 partitions (source A side)");
            assertTrue(sidB >= 0, "expected one sub-topology with 3 partitions (post-repartition side)");
            assertNotEqualsInt(sidA, sidB, "the two sub-topologies must be distinct");

            // Both sinks emit. outA produced (k, 2L) and outB produced (k2, "vb").
            final TestOutputTopic<String, Long> readA =
                driver.createOutputTopic(outA, STRING_DES, new LongDeserializer());
            final TestOutputTopic<String, String> readB =
                driver.createOutputTopic(outB, STRING_DES, STRING_DES);
            assertFalse(readA.isEmpty(), "outA should have received a record");
            assertFalse(readB.isEmpty(), "outB should have received a record (post-repartition)");
        }
    }

    private static void assertNotEqualsInt(final int a, final int b, final String message) {
        if (a == b) {
            throw new AssertionError(message + ": both equal " + a);
        }
    }

    /**
     * PAPI processor used by
     * {@link #twoSourcesTwoSinksDifferentPartitionsAndExplicitRepartitionAndSharedStore}.
     * Increments the {@code shared} key-value store on every record and forwards the new count.
     */
    private static final class SharedStoreCounter implements Processor<String, String, String, Long> {
        private KeyValueStore<String, Long> store;
        private ProcessorContext<String, Long> ctx;

        @Override
        public void init(final ProcessorContext<String, Long> context) {
            this.ctx = context;
            this.store = context.getStateStore("shared");
        }

        @Override
        public void process(final Record<String, String> record) {
            final Long current = store.get(record.key());
            final long next = (current == null ? 0L : current) + 1L;
            store.put(record.key(), next);
            ctx.forward(record.withValue(next));
        }
    }

    /**
     * Two sources with different partition counts ({@code inA}=4, {@code inB}=2) merged into one
     * sub-topology that increments a shared {@code counts} store. The {@code inB} side is aligned
     * to 4 partitions via {@link Repartitioned#withNumberOfPartitions(int)} so the merge
     * co-partitions. The same key arriving on both sides must land in the same store partition
     * (because {@link StringSerializer} ignores the topic name, so {@code murmur2 % 4} matches
     * across {@code inA} and the repartition topic).
     */
    @Test
    public void coAlignedKeysAggregateAcrossSourcesAfterRepartition() {
        final String inA = "inA";  // 4 partitions
        final String inB = "inB";  // 2 partitions, repartitioned up to 4 below

        final StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("counts"),
            Serdes.String(),
            Serdes.Long()));

        final KStream<String, String> streamA =
            builder.stream(inA, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> streamB =
            builder.stream(inB, Consumed.with(Serdes.String(), Serdes.String()));

        final KStream<String, String> alignedB = streamB
            .repartition(Repartitioned.<String, String>with(Serdes.String(), Serdes.String())
                .withNumberOfPartitions(4));

        streamA.merge(alignedB).process(CountIncrementer::new, "counts");

        // With two source partitions feeding one merged sub-topology, the default
        // max.task.idle.ms=0 would let a task stall waiting for the side that has not been
        // pipeInput'd yet. -1 disables idling so each pipeInput drains immediately.
        final Properties props = baseProps();
        props.setProperty(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, "-1");

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.declareTopic(inA, 4);
            driver.declareTopic(inB, 2);
            driver.init();

            final TestInputTopic<String, String> pipeA =
                driver.createInputTopic(inA, STRING_SER, STRING_SER);
            final TestInputTopic<String, String> pipeB =
                driver.createInputTopic(inB, STRING_SER, STRING_SER);

            pipeA.pipeInput("alignedKey", "from-A");
            pipeB.pipeInput("alignedKey", "from-B");
            pipeA.pipeInput("onlyA", "from-A");
            pipeB.pipeInput("onlyB", "from-B");

            assertEquals(4, driver.partitionsOf("counts"));

            final int alignedPart = BuiltInPartitioner.partitionForKey(STRING_SER.serialize(inA, "alignedKey"), 4);
            final int onlyAPart   = BuiltInPartitioner.partitionForKey(STRING_SER.serialize(inA, "onlyA"), 4);
            final int onlyBPart   = BuiltInPartitioner.partitionForKey(STRING_SER.serialize(inA, "onlyB"), 4);

            assertEquals(Long.valueOf(2L),
                driver.<String, Long>getKeyValueStore("counts", alignedPart).get("alignedKey"),
                "alignedKey from inA and inB must co-locate on partition " + alignedPart);
            assertEquals(Long.valueOf(1L),
                driver.<String, Long>getKeyValueStore("counts", onlyAPart).get("onlyA"));
            assertEquals(Long.valueOf(1L),
                driver.<String, Long>getKeyValueStore("counts", onlyBPart).get("onlyB"));

            // No leakage: alignedKey must not appear in any other partition. This is what
            // proves the inB repartition actually aligned onto inA's layout — without it,
            // the inA copy and the inB copy would land in different partitions.
            for (int p = 0; p < 4; p++) {
                if (p != alignedPart) {
                    assertNull(driver.<String, Long>getKeyValueStore("counts", p).get("alignedKey"),
                        "alignedKey leaked to partition " + p);
                }
            }
        }
    }

    /**
     * Footgun documentation: two sources with mismatching partition counts ({@code inA}=2,
     * {@code inB}=3) feed the same {@code counts} store via {@link KStream#merge(KStream)} +
     * {@code process()} — without any explicit {@link Repartitioned} alignment.
     *
     * <p>Production Kafka Streams does NOT raise a co-partition error here: the DSL only
     * registers co-partition groups for joins (KStream-KStream, KStream-KTable, foreign-key),
     * not for shared-store merges. The KIP-1238 {@link TopologyTestDriver} inherits the same
     * blind spot, so {@code validateCopartitioning()} finds nothing to check and
     * {@code init()} succeeds. The merged sub-topology then runs at
     * {@code max(2, 3) = 3} partitions, and the same key arriving on both sources lands in
     * two different store instances ({@code murmur2(k) % 2} on inA vs {@code murmur2(k) % 3}
     * on inB), silently splitting the per-key state.</p>
     *
     * <p>This test pins that behaviour so any future change — stricter validation that throws,
     * or a routing change that quietly fixes the split — is caught.</p>
     */
    @Test
    public void mismatchedPartitionsAcrossSharedStoreSourcesSilentlySplitState() {
        final String inA = "inA";  // 2 partitions
        final String inB = "inB";  // 3 partitions

        final StreamsBuilder builder = new StreamsBuilder();
        builder.addStateStore(Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("counts"),
            Serdes.String(),
            Serdes.Long()));

        final KStream<String, String> streamA =
            builder.stream(inA, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> streamB =
            builder.stream(inB, Consumed.with(Serdes.String(), Serdes.String()));

        // No Repartitioned.withNumberOfPartitions(): both sources feed the same sub-topology
        // and share the "counts" store, but no co-partition constraint is registered.
        streamA.merge(streamB).process(CountIncrementer::new, "counts");

        final Properties props = baseProps();
        props.setProperty(StreamsConfig.MAX_TASK_IDLE_MS_CONFIG, "-1");

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            driver.declareTopic(inA, 2);
            driver.declareTopic(inB, 3);
            // init() must NOT throw: the DSL did not flag this merge as co-partitioned.
            driver.init();

            // Sub-topology partition count = max(2, 3) = 3.
            assertEquals(3, driver.partitionsOf("counts"));

            // Pick a key whose inA-side partition (murmur2 % 2) differs from its inB-side
            // partition (murmur2 % 3), so the silent split is actually observable.
            String key = null;
            int partA = -1;
            int partB = -1;
            for (final String candidate : new String[] {"k", "key-1", "alpha", "beta", "gamma", "delta", "epsilon", "zeta"}) {
                final int a = BuiltInPartitioner.partitionForKey(STRING_SER.serialize(inA, candidate), 2);
                final int b = BuiltInPartitioner.partitionForKey(STRING_SER.serialize(inB, candidate), 3);
                if (a != b) {
                    key = candidate;
                    partA = a;
                    partB = b;
                    break;
                }
            }
            assertNotNull(key, "test setup: expected at least one candidate key with diverging partA/partB");

            final TestInputTopic<String, String> pipeA =
                driver.createInputTopic(inA, STRING_SER, STRING_SER);
            final TestInputTopic<String, String> pipeB =
                driver.createInputTopic(inB, STRING_SER, STRING_SER);

            pipeA.pipeInput(key, "from-A");
            pipeB.pipeInput(key, "from-B");

            // The split: each side wrote to its own task's store instance, never the other.
            assertEquals(Long.valueOf(1L),
                driver.<String, Long>getKeyValueStore("counts", partA).get(key),
                "inA must have incremented its own store partition " + partA);
            assertEquals(Long.valueOf(1L),
                driver.<String, Long>getKeyValueStore("counts", partB).get(key),
                "inB must have incremented its own store partition " + partB);

            // No co-localization: total across all 3 store partitions is 2, not (e.g.) 1+2 or 2+0.
            long sum = 0;
            for (int p = 0; p < 3; p++) {
                final Long v = driver.<String, Long>getKeyValueStore("counts", p).get(key);
                if (v != null) {
                    sum += v;
                }
            }
            assertEquals(2L, sum,
                "the two single-source increments must have stayed isolated, total = 2");
        }
    }

    /**
     * PAPI processor used by {@link #coAlignedKeysAggregateAcrossSourcesAfterRepartition} and
     * {@link #mismatchedPartitionsAcrossSharedStoreSourcesSilentlySplitState}. Increments
     * {@code counts[key]} on every input record. No downstream forward.
     */
    private static final class CountIncrementer implements Processor<String, String, Void, Void> {
        private KeyValueStore<String, Long> store;

        @Override
        public void init(final ProcessorContext<Void, Void> context) {
            this.store = context.getStateStore("counts");
        }

        @Override
        public void process(final Record<String, String> record) {
            final Long current = store.get(record.key());
            store.put(record.key(), (current == null ? 0L : current) + 1L);
        }
    }
}
