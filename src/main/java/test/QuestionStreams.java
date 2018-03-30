package test;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import streams_dsl.MyEventTimeExtractor;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class QuestionStreams {

    final static private Logger logger = Logger.getLogger(QuestionStreams.class);

    public static void main(String[] args) throws Exception{

        QuestionStreams questionStreams = new QuestionStreams();
        questionStreams.start();

    }

    private void start() throws Exception {

        Properties properties = new Properties();
        properties.load(QuestionStreams.class.getClassLoader().getResourceAsStream("config.properties"));
        String redisUrl = properties.getProperty("redis.url");
        String redisAuth = properties.getProperty("redis.auth");
        String mysqlDiver = properties.getProperty("mysql.driver");
        String mysqlUrl = properties.getProperty("mysql.url");
        String mysqlUser = properties.getProperty("mysql.user");
        String mysqlAuth = properties.getProperty("mysql.auth");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-question");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "master:9092,slave01:9092,slave02:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyEventTimeExtractor.class.getName());

        // Redis 客户端连接
        final Jedis jedis = new Jedis(redisUrl);
        jedis.auth(redisAuth);
        // Mysql 客户端连接
        Class.forName(mysqlDiver);
        final Connection conn = DriverManager.getConnection( mysqlUrl, mysqlUser, mysqlAuth);
        final Statement statement = conn.createStatement();


        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("question");
        final SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        // 从 topic 中取出 question 数据，计算完毕后放入 Redis 的有序集合中
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

            double score_base = ( follower + 1 ) * 0.2 + Math.log10( view );
            double score_accelerationScore = 0L;
            double score_denominator =0L;
            double sumScore = 0L;

            try {
                score_accelerationScore = (
                    (follower - lastFollower + 0.1)/
                        Math.log10((fmt.parse(insertTime).getTime() - fmt.parse(lastInsertTime).getTime())/1000) * 0.5 +
                        Math.log10(view - lastView + 0.1)/ Math.log10((fmt.parse(insertTime).getTime() - fmt.parse(lastInsertTime).getTime())/1000)
                    )*86400;
                score_denominator = Math.pow((new Date().getTime() - fmt.parse(createTime).getTime())/2 + 1, 1.5);

                sumScore =  score_base * score_accelerationScore / score_denominator;
                logger.info(id + " : " + sumScore);
                sumScore = Double.isNaN(sumScore) ? 0L : sumScore;

            } catch (ParseException e) {
                logger.error("计算分数异常: ", e);
            }

            try {
                // 将计算机结果放入 Redis 有序集合，集合元素数量超过1000，则删除分数最低的元素
                jedis.zadd("zhihu:top", sumScore, id);
                long tmp = jedis.zcard("zhihu:top") - 1000L;
                if (tmp > 0) {
                    jedis.zremrangeByRank("zhihu:top", 0, tmp);
                }

                // 将分数 update 到 mysql 中
                statement.execute("UPDATE question_measure SET score='"+sumScore+"' WHERE id='"+ id +"' AND insert_time='"+insertTime+"'");

            } catch (Exception e) {
                logger.error("Mysql更新数据异常: ", e);
            }
        });


        final Topology topology = builder.build();
        logger.info(topology.describe());

        // attach shutdown handler to catch control-c
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                try {
                    statement.close();
                    conn.close();
                } catch (SQLException e) {
                    logger.error("Mysql关闭异常: ", e);
                }
                jedis.close();
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
