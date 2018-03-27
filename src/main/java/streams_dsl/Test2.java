package streams_dsl;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * 高级Streams DSL
 */
public class Test2 {
    public static void main(String[] args){
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-linesplit");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "slave02:9092");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,MyEventTimeExtractor.class.getName());

        final StreamsBuilder builder = new StreamsBuilder();

//        KStream<String, String> source = builder.stream("streams-plaintext-input");
//        KStream<String, String> words = source.flatMapValues(value -> Arrays.asList(value.split("\\W+")));
        KStream<String, String> source = builder.stream("zhihu_question");
        KStream<String, String> words = source.flatMapValues(new ValueMapper<String, Iterable<String>>() {

            @Override
            public Iterable<String> apply(String value) {
                System.out.println(value);
                return Arrays.asList(value.split("\\W+"));
            }
        });
        words.to("test2");


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
