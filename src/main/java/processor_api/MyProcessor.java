package processor_api;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * 底层处理API
 */
public class MyProcessor implements Processor<String, String> {


    private ProcessorContext context;
    private KeyValueStore<String, Long> kvStore;

    /**
     * provides access to the metadata of the currently processed record,
     * including its source Kafka topic and partition,
     *          its corresponding message offset,
     *          and further such information
     * You can also use this context instance
     *          to schedule a punctuation function (via ProcessorContext#schedule()),
     *          to forward a new record as a key-value pair
     *          to the downstream processors (via ProcessorContext#forward()),
     *          and to commit the current processing progress (via ProcessorContext#commit()
     * @param processorContext
     */
    @Override
    public void init(ProcessorContext processorContext) {
        // keep the processor context locally because we need it in punctuate() and commit()
        this.context = processorContext;

        // retrieve the key-value store named "Counts"
        this.kvStore = (KeyValueStore) context.getStateStore("Counts");

        // schedule a punctuate() method every 1000 milliseconds based on stream-time
        this.context.schedule(2000, PunctuationType.STREAM_TIME, (timestamp) -> {
            KeyValueIterator<String, Long> iter = this.kvStore.all();
            while (iter.hasNext()) {
                KeyValue<String, Long> entry = iter.next();
                context.forward(entry.key, entry.value.toString());
                System.out.println("schedule....  " + entry.key + ":" + entry.value.toString());
            }
            iter.close();

            // commit the current processing progress
            context.commit();
        });
    }

    /**
     * called on each of the received records
     * @param dummy
     * @param line
     */
    @Override
    public void process(String dummy, String line) {
//        String[] words = line.toLowerCase().split(" ");
//
//        for (String word : words) {
//            Long oldValue = this.kvStore.get(word);
//
//            if (oldValue == null) {
//                this.kvStore.put(word, 1L);
//            } else {
//                this.kvStore.put(word, oldValue + 1);
//            }
//        }
        System.out.println("process[line]  "+line);
        System.out.println("process[dummy]  "+dummy);
        this.kvStore.put("word", 1L);
    }

    @Override
    public void punctuate(long timestamp) {

    }

    @Override
    public void close() {
        this.kvStore.close();
    }


}
