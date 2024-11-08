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

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class MultiPartitionTest {
    private static final String INPUT_TOPIC = "input";
    private static final String INPUT2_TOPIC = "input2";
    private static final String OUTPUT_TOPIC = "output1";

    private TopologyTestDriver testDriver;
    private final Serde<String> stringSerde = new Serdes.StringSerde();
    private final Serde<Long> longSerde = new Serdes.LongSerde();

    private final Instant testBaseTime = Instant.parse("2019-06-01T10:00:00Z");

    private static final class ValueProcessor extends ContextualProcessor<String, String, String, Long> {

        private KeyValueStore<String, Long> concatStore;

        @Override
        public void init(final ProcessorContext<String, Long> context) {
            super.init(context);

            concatStore = context.getStateStore("store");
        }

        @Override
        public void process(final Record<String, String> record) {

            final var stateKey = record.value();

            var existingState = concatStore.get(stateKey);
            if (existingState != null) {
                existingState += 1;
            } else {
                existingState = 1L;
            }

            concatStore.put(stateKey, existingState);
            context().forward(record.withKey(stateKey).withValue(existingState));
        }
    }

    @BeforeEach
    public void setup() {

    }

    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void testSinglePartition() {
        final StreamsBuilder builder = new StreamsBuilder();
        //Create Actual Stream Processing pipeline

        // Create String/String KeyValue statestore
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("store"),
                        stringSerde,
                        longSerde));

        final var stream1 = builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, stringSerde));
        final var stream2 = builder.stream(INPUT2_TOPIC, Consumed.with(stringSerde, stringSerde));

        stream1.process(ValueProcessor::new, "store").to(OUTPUT_TOPIC, Produced.with(stringSerde, longSerde));
        stream2.process(ValueProcessor::new, "store").to(OUTPUT_TOPIC, Produced.with(stringSerde, longSerde));

        final Properties properties = new Properties();
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);

        testDriver = new TopologyTestDriver(builder.build(), properties);

        final TestInputTopic<String, String> inputTopic =
                testDriver.createInputTopic(INPUT_TOPIC, stringSerde.serializer(), stringSerde.serializer());
        final TestInputTopic<String, String> inputTopic2 =
                testDriver.createInputTopic(INPUT2_TOPIC, stringSerde.serializer(), stringSerde.serializer());

        final TestOutputTopic<String, Long> outputTopic =
            testDriver.createOutputTopic(OUTPUT_TOPIC, stringSerde.deserializer(), longSerde.deserializer());

        inputTopic.pipeInput("k1", "Hello");
        inputTopic2.pipeInput("k1", "World");

        final var result = outputTopic.readKeyValuesToList();
        assertThat(result.get(0).key, equalTo("Hello"));
        assertThat(result.get(1).key, equalTo("World"));
        assertThat(result.get(0).value, equalTo(1L));
        assertThat(result.get(1).value, equalTo(1L));

        inputTopic.pipeInput("k1", "Hello");
        inputTopic2.pipeInput("k2", "World");

        final var result2 = outputTopic.readKeyValuesToList();
        assertThat(result2.get(0).key, equalTo("Hello"));
        assertThat(result2.get(1).key, equalTo("World"));
        assertThat(result2.get(0).value, equalTo(2L));
        assertThat(result2.get(1).value, equalTo(2L));

        //No more output in topic
        assertThat(outputTopic.isEmpty(), is(true));
    }

    @Test
    public void testMultiPartition() {
        final StreamsBuilder builder = new StreamsBuilder();
        //Create Actual Stream Processing pipeline

        // Create String/String KeyValue statestore
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("store"),
                        stringSerde,
                        longSerde));

        final var stream1 = builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, stringSerde));
        final var stream2 = builder.stream(INPUT2_TOPIC, Consumed.with(stringSerde, stringSerde));

        stream1.process(ValueProcessor::new, "store").to(OUTPUT_TOPIC, Produced.with(stringSerde, longSerde));
        stream2.process(ValueProcessor::new, "store").to(OUTPUT_TOPIC, Produced.with(stringSerde, longSerde));

        final Properties properties = new Properties();
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);

        testDriver = new TopologyTestDriver(builder.build(), properties, testBaseTime, 3);

        final TestInputTopic<String, String> inputTopic =
                testDriver.createInputTopic(INPUT_TOPIC, stringSerde.serializer(), stringSerde.serializer());
        final TestInputTopic<String, String> inputTopic2 =
                testDriver.createInputTopic(INPUT2_TOPIC, stringSerde.serializer(), stringSerde.serializer());

        final TestOutputTopic<String, Long> outputTopic =
                testDriver.createOutputTopic(OUTPUT_TOPIC, stringSerde.deserializer(), longSerde.deserializer());

        inputTopic.pipeInput("k1", "Hello");
        inputTopic2.pipeInput("k1", "World");

        final var result = outputTopic.readKeyValuesToList();
        assertThat(result.get(0).key, equalTo("Hello"));
        assertThat(result.get(1).key, equalTo("World"));
        assertThat(result.get(0).value, equalTo(1L));
        assertThat(result.get(1).value, equalTo(1L));

        inputTopic.pipeInput("k1", "Hello");
        inputTopic2.pipeInput("k2", "World");

        final var result2 = outputTopic.readKeyValuesToList();
        assertThat(result2.get(0).key, equalTo("Hello"));
        assertThat(result2.get(1).key, equalTo("World"));
        assertThat(result2.get(0).value, equalTo(2L));
        assertThat(result2.get(1).value, equalTo(1L));

        //No more output in topic
        assertThat(outputTopic.isEmpty(), is(true));
    }

    @Test
    public void testMultiPartitionRepartition() {
        final StreamsBuilder builder = new StreamsBuilder();
        //Create Actual Stream Processing pipeline

        // Create String/String KeyValue statestore
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("store"),
                        stringSerde,
                        longSerde));

        final var stream1 = builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, stringSerde))
                .selectKey((k, v) -> v)
                .repartition(Repartitioned.with(stringSerde, stringSerde));
        final var stream2 = builder.stream(INPUT2_TOPIC, Consumed.with(stringSerde, stringSerde))
                .selectKey((k, v) -> v)
                .repartition(Repartitioned.with(stringSerde, stringSerde));

        stream1.process(ValueProcessor::new, "store").to(OUTPUT_TOPIC, Produced.with(stringSerde, longSerde));
        stream2.process(ValueProcessor::new, "store").to(OUTPUT_TOPIC, Produced.with(stringSerde, longSerde));

        final Properties properties = new Properties();
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);

        testDriver = new TopologyTestDriver(builder.build(), properties, testBaseTime, 3);

        final TestInputTopic<String, String> inputTopic =
                testDriver.createInputTopic(INPUT_TOPIC, stringSerde.serializer(), stringSerde.serializer());
        final TestInputTopic<String, String> inputTopic2 =
                testDriver.createInputTopic(INPUT2_TOPIC, stringSerde.serializer(), stringSerde.serializer());

        final TestOutputTopic<String, Long> outputTopic =
                testDriver.createOutputTopic(OUTPUT_TOPIC, stringSerde.deserializer(), longSerde.deserializer());

        inputTopic.pipeInput("k1", "Hello");
        inputTopic2.pipeInput("k1", "World");

        final var result = outputTopic.readKeyValuesToList();
        assertThat(result.get(0).key, equalTo("Hello"));
        assertThat(result.get(1).key, equalTo("World"));
        assertThat(result.get(0).value, equalTo(1L));
        assertThat(result.get(1).value, equalTo(1L));

        inputTopic.pipeInput("k1", "Hello");
        inputTopic2.pipeInput("k2", "World");

        final var result2 = outputTopic.readKeyValuesToList();
        assertThat(result2.get(0).key, equalTo("Hello"));
        assertThat(result2.get(1).key, equalTo("World"));
        assertThat(result2.get(0).value, equalTo(2L));
        assertThat(result2.get(1).value, equalTo(2L));

        //No more output in topic
        assertThat(outputTopic.isEmpty(), is(true));
    }

    @Test
    public void testMultiPartitionWrongRepartition() {
        final StreamsBuilder builder = new StreamsBuilder();
        //Create Actual Stream Processing pipeline

        // Create String/String KeyValue statestore
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.persistentKeyValueStore("store"),
                        stringSerde,
                        longSerde));

        final var stream1 = builder.stream(INPUT_TOPIC, Consumed.with(stringSerde, stringSerde))
                .selectKey((k, v) -> k) // keep the key as partition driver, should not aggregate properly
                .repartition(Repartitioned.with(stringSerde, stringSerde));
        final var stream2 = builder.stream(INPUT2_TOPIC, Consumed.with(stringSerde, stringSerde))
                .selectKey((k, v) -> k) // keep the key as partition driver, should not aggregate properly
                .repartition(Repartitioned.with(stringSerde, stringSerde));

        stream1.process(ValueProcessor::new, "store").to(OUTPUT_TOPIC, Produced.with(stringSerde, longSerde));
        stream2.process(ValueProcessor::new, "store").to(OUTPUT_TOPIC, Produced.with(stringSerde, longSerde));

        final Properties properties = new Properties();
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArraySerde.class);

        testDriver = new TopologyTestDriver(builder.build(), properties, testBaseTime, 3);

        final TestInputTopic<String, String> inputTopic =
                testDriver.createInputTopic(INPUT_TOPIC, stringSerde.serializer(), stringSerde.serializer());
        final TestInputTopic<String, String> inputTopic2 =
                testDriver.createInputTopic(INPUT2_TOPIC, stringSerde.serializer(), stringSerde.serializer());

        final TestOutputTopic<String, Long> outputTopic =
                testDriver.createOutputTopic(OUTPUT_TOPIC, stringSerde.deserializer(), longSerde.deserializer());

        inputTopic.pipeInput("k1", "Hello");
        inputTopic2.pipeInput("k1", "World");

        final var result = outputTopic.readKeyValuesToList();
        assertThat(result.get(0).key, equalTo("Hello"));
        assertThat(result.get(1).key, equalTo("World"));
        assertThat(result.get(0).value, equalTo(1L));
        assertThat(result.get(1).value, equalTo(1L));

        inputTopic.pipeInput("k1", "Hello");
        inputTopic2.pipeInput("k2", "World");

        final var result2 = outputTopic.readKeyValuesToList();
        assertThat(result2.get(0).key, equalTo("Hello"));
        assertThat(result2.get(1).key, equalTo("World"));
        assertThat(result2.get(0).value, equalTo(2L));
        assertThat(result2.get(1).value, equalTo(1L));

        //No more output in topic
        assertThat(outputTopic.isEmpty(), is(true));
    }
}
