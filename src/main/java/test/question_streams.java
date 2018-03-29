package test;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import redis.clients.jedis.Jedis;
import streams_dsl.MyEventTimeExtractor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class question_streams {
    public static void main(String[] args){

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-question");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "slave02:9092");

        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyEventTimeExtractor.class.getName());

        final StreamsBuilder builder = new StreamsBuilder();
        final SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        final Jedis jedis = new Jedis("140.143.206.106", 6379);
        jedis.auth("");

        KStream<String, String> source = builder.stream("question");

        /*
         * 从 topic 中取出 question 数据，计算完毕后放入 Redis 的有序集合中
         */
        source.foreach((String key, String value) -> {

            JSONObject jsonObject = JSONObject.parseObject(value);

            String id = jsonObject.getString("id");
            String createTime = jsonObject.getString("create_time");
            String insertTime = jsonObject.getString("insert_time");
            String lastInsertTime = jsonObject.getString("last_insert_time");

            long follower = jsonObject.getLongValue("follower");
            long view = jsonObject.getLongValue("view");

            long lastFollower = jsonObject.getLongValue("last_follower");
            long lastView = jsonObject.getLongValue("last_view");

//            ( ((t1.follower_count+1)*0.2 + LOG10(t1.view_count))    *
//                    ((t1.follower_count-t2.follower_count+0.1)/log10(TIMESTAMP(t1.insert_time)-TIMESTAMP (t2.insert_time))*0.5 +
//                            log10(t1.view_count-t2.view_count+0.1)/log10(TIMESTAMP(t1.insert_time)-TIMESTAMP (t2.insert_time))
//                    ))*86400 / POW(unix_timestamp(now())/2-unix_timestamp(t0.create_time)/2 + 1,1.5) as "总分",

            double score_base = ( follower + 1 ) * 0.2 + Math.log10( view );
            double score_accelerationScore = 0L;
            double score_denominator = 0L;

            try {
                score_accelerationScore = ((follower-lastFollower+0.1)/
                        Math.log10(fmt.parse(insertTime).getTime()/1000-fmt.parse(lastInsertTime).getTime()/1000)*0.5 +
                        Math.log10(view-lastView+0.1)/Math.log10(fmt.parse(insertTime).getTime()/1000-fmt.parse(lastInsertTime).getTime()/1000)
                )*86400;
                score_denominator = Math.pow(new Date().getTime()/2-fmt.parse(createTime).getTime()/2 + 1,1.5);

            } catch (ParseException e) {
                e.printStackTrace();
            }

            double sumScore =  score_base * score_accelerationScore / score_denominator;
            System.out.println(id+" : "+sumScore);

            // TODO

        });



        final Topology topology = builder.build();
        System.out.println(topology.describe());


        // attach shutdown handler to catch control-c
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {

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
