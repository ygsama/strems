package processor_api;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import streams_dsl.MyEventTimeExtractor;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Test4 {
    public static void main(String[] args){

        Topology builder = new Topology();

        StoreBuilder<KeyValueStore<String, Long>> countStoreSupplier =
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("Counts"),
                        Serdes.String(),
                        Serdes.Long())
                        .withLoggingDisabled();


        // add the source processor node that takes Kafka topic "zhihu_question" as input
        Topology topology = builder.addSource("Source", "zhihu_question")

                // add the WordCountProcessor node which takes the source processor as its upstream processor
                .addProcessor("Process", MyProcessor::new, "Source")

                // add the count store associated with the WordCountProcessor processor
                .addStateStore(countStoreSupplier, "Process")

                // add the sink processor node that takes Kafka topic "test2" as output
                // and the WordCountProcessor node as its upstream processor
                .addSink("Sink", "test2", "Process");



        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "slave02:9092");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyEventTimeExtractor.class.getName());

        System.out.println(topology.describe());

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
