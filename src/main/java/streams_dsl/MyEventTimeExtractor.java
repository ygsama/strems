package streams_dsl;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class MyEventTimeExtractor  implements TimestampExtractor {

    public long extract(ConsumerRecord<Object, Object> consumerRecord, long l) {
        return System.currentTimeMillis();
    }
}
