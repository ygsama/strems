package test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class test {
    public static void main(String[] args) throws Exception {

//       testTimestamp();


         new test().testProp();

    }

    void testProp() throws IOException {
        Properties properties = new Properties();
        properties.load(getClass().getClassLoader().getResourceAsStream("log4j.properties"));

        System.out.println(properties.get("log4j.rootLogger"));
    }

    static void testTimestamp() throws ParseException {
        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Date date = fmt.parse("2018-01-01 10:20:20");
        System.out.println(date.getTime()/1000);
        System.out.println(date.getTime());


    }
    /**
     * 测试算法
     */
    static void testScore(){
//        {
//            'id':'45147755',
//                'answer':'3876',
//                'coment':'45',
//                'view':'7821408',
//                'follower':'12435',
//                'last_answer':'3878',
//                'last_coment':'45',
//                'last_follower':'12440',
//                'last_view':'7832938'
//        }

        SimpleDateFormat fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String id = "45147755";
        String createTime = "2016-05-01 20:10:22";

        long follower = 12435L;
        long view = 7821408L;
        String insertTime = "2018-03-27 10:58:46";

        long lastFollower = 12435L;
        long lastView = 7820423L;
        String lastInsertTime = "2018-03-27 08:56:42";

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

        System.out.println(sumScore);
    }
}
