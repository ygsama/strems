package streams_dsl;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * 高级Streams DSL
 */
public class Test3 {
    public static void main(String[] args){

        Properties props = new Properties();
            props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "slave02:9092");

            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,MyEventTimeExtractor.class.getName());

        final StreamsBuilder builder = new StreamsBuilder();

//        KStream<String, String> source = builder.stream("streams-plaintext-input");
//        source.flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
//                .groupBy((key, value) -> value)
//                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"))
//                .toStream()
//                .to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long());

        KStream<String, String> source = builder.stream("zhihu_question");
        KTable<String, Long> counts = source.flatMapValues(new ValueMapper<String, Iterable<String>>() {
                @Override
                public Iterable<String> apply(String value) {
                    return Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"));
                }
            }).groupBy(new KeyValueMapper<String, String, String>() {
                @Override
                public String apply(String key, String value) {
                    System.out.println(value);
                    return value;
                }
            }).count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>> as("counts-store"));

        counts.toStream().to("test2", Produced.with(Serdes.String(), Serdes.Long()));

        final Topology topology = builder.build();
        System.out.println(topology.describe());


        // attach shutdown handler to catch control-c
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                // 用来关闭streams线程
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
            System.exit(0);
    }
}
