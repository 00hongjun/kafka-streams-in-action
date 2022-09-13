package bbejeck.chapter_4.partitioner;

import bbejeck.model.Purchase;
import org.apache.kafka.streams.processor.StreamPartitioner;


public class RewardsStreamPartitioner implements StreamPartitioner<String, Purchase> {

    @Override
    public Integer partition(String key, Purchase value, int numPartitions) {
        return value.getCustomerId().hashCode() % numPartitions; /** 고객 id를 이용해 파티셔닝한다 */
    }
}
