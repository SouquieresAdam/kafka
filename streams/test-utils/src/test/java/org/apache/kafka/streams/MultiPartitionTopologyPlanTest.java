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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.errors.TopologyException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link MultiPartitionTopologyPlan}, the pure planning layer of the multi-partition
 * {@link TopologyTestDriver}. These exercise the layout computation directly — task sub-topology
 * enumeration, the layered repartition-count resolution, co-partition validation and per-sub-topology
 * partition counts — without constructing a driver, piping records, or touching state stores.
 */
public class MultiPartitionTopologyPlanTest {

    /**
     * Build an (un-computed) plan for a topology, reproducing the minimal builder initialisation the
     * driver performs before {@link TopologyTestDriver#init()} runs.
     */
    private static MultiPartitionTopologyPlan newPlan(final Topology topology, final Map<String, Integer> declared) {
        final InternalTopologyBuilder builder = topology.internalTopologyBuilder;
        final Properties props = new Properties();
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "plan-test");
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        builder.rewriteTopology(new StreamsConfig(props));
        builder.buildTopology();
        return new MultiPartitionTopologyPlan(builder, builder.buildGlobalStateTopology(), declared);
    }

    @Test
    public void sourcePartitionCountPropagatesThroughRepartitionToDownstreamSubtopology() {
        // selectKey forces a repartition before the count. With the source declared at 4 partitions,
        // the repartition topic inherits 4 (upstream-max), so both task sub-topologies run at 4.
        final StreamsBuilder sb = new StreamsBuilder();
        sb.stream("input", Consumed.with(Serdes.String(), Serdes.String()))
            .selectKey((k, v) -> v)
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .count(Materialized.as("counts"));

        final MultiPartitionTopologyPlan plan = newPlan(sb.build(), Map.of("input", 4));
        plan.compute();

        assertEquals(2, plan.subtopologyIds().size(), "selectKey + count yields two task sub-topologies");
        for (final int sid : plan.subtopologyIds()) {
            assertEquals(4, plan.partitionsOfSubtopology(sid),
                "sub-topology " + sid + " should run at 4 partitions");
        }
    }

    @Test
    public void explicitRepartitionNumberOfPartitionsWinsOverUpstreamMax() {
        // inB is declared at 2, but the repartition pins 3; the post-repartition sub-topology must run
        // at 3, while the source-side sub-topology keeps inB's 2.
        final StreamsBuilder sb = new StreamsBuilder();
        sb.stream("inB", Consumed.with(Serdes.String(), Serdes.String()))
            .repartition(Repartitioned.<String, String>with(Serdes.String(), Serdes.String())
                .withNumberOfPartitions(3))
            .to("outB", Produced.with(Serdes.String(), Serdes.String()));

        final MultiPartitionTopologyPlan plan = newPlan(sb.build(), Map.of("inB", 2));
        plan.compute();

        assertTrue(plan.subtopologyIds().stream().anyMatch(sid -> plan.partitionsOfSubtopology(sid) == 3),
            "the post-repartition sub-topology should run at the pinned 3 partitions");
        assertTrue(plan.subtopologyIds().stream().anyMatch(sid -> plan.partitionsOfSubtopology(sid) == 2),
            "the inB source sub-topology should run at its declared 2 partitions");
    }

    @Test
    public void globalSourceTopicIsExcludedFromTaskSubtopologies() {
        // The global table's node group has only a global source, so it is not a task sub-topology.
        final StreamsBuilder sb = new StreamsBuilder();
        final GlobalKTable<String, String> dim = sb.globalTable("dim",
            Consumed.with(Serdes.String(), Serdes.String()), Materialized.as("dimStore"));
        sb.stream("facts", Consumed.with(Serdes.String(), Serdes.String()))
            .join(dim, (factKey, factVal) -> factKey, (factVal, dimVal) -> factVal + dimVal)
            .to("out", Produced.with(Serdes.String(), Serdes.String()));

        final MultiPartitionTopologyPlan plan = newPlan(sb.build(), Map.of("facts", 4));
        plan.compute();

        assertNotNull(plan.subtopologyForInputTopic("facts"), "facts is a task sub-topology source");
        assertNull(plan.subtopologyForInputTopic("dim"),
            "dim is global and must not be a task sub-topology source");
        assertEquals(4, plan.partitionsOfSubtopology(plan.subtopologyForInputTopic("facts")));
    }

    @Test
    public void coPartitionMismatchThrowsNamingBothTopics() {
        // A KStream-KStream join co-partitions its inputs; declaring them at different counts is invalid.
        final StreamsBuilder sb = new StreamsBuilder();
        final KStream<String, String> a = sb.stream("inA", Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> b = sb.stream("inB", Consumed.with(Serdes.String(), Serdes.String()));
        a.join(b,
                (va, vb) -> va + vb,
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofMinutes(1)),
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String()))
            .to("out", Produced.with(Serdes.String(), Serdes.String()));

        final MultiPartitionTopologyPlan plan = newPlan(sb.build(), Map.of("inA", 2, "inB", 3));

        final TopologyException e = assertThrows(TopologyException.class, plan::compute);
        assertTrue(e.getMessage().contains("inA") && e.getMessage().contains("inB"),
            "co-partition error should name both witnessing topics, was: " + e.getMessage());
    }

    @Test
    public void disjointSubtopologiesKeepHeterogeneousPartitionCounts() {
        final StreamsBuilder sb = new StreamsBuilder();
        sb.stream("inA", Consumed.with(Serdes.String(), Serdes.String()))
            .to("outA", Produced.with(Serdes.String(), Serdes.String()));
        sb.stream("inB", Consumed.with(Serdes.String(), Serdes.String()))
            .to("outB", Produced.with(Serdes.String(), Serdes.String()));

        final MultiPartitionTopologyPlan plan = newPlan(sb.build(), Map.of("inA", 4, "inB", 2));
        plan.compute();

        assertEquals(4, plan.partitionsOfSubtopology(plan.subtopologyForInputTopic("inA")));
        assertEquals(2, plan.partitionsOfSubtopology(plan.subtopologyForInputTopic("inB")));
    }

    @Test
    public void undeclaredTopicsDefaultToOnePartition() {
        final StreamsBuilder sb = new StreamsBuilder();
        sb.stream("input", Consumed.with(Serdes.String(), Serdes.String()))
            .to("output", Produced.with(Serdes.String(), Serdes.String()));

        final MultiPartitionTopologyPlan plan = newPlan(sb.build(), Map.of());
        plan.compute();

        assertEquals(1, plan.subtopologyIds().size());
        assertEquals(1, plan.partitionsOfSubtopology(plan.subtopologyForInputTopic("input")));
        assertEquals(1, plan.partitionsOfTopic("input"));
    }
}
