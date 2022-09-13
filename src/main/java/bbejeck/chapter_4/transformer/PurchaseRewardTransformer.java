package bbejeck.chapter_4.transformer;

import bbejeck.model.Purchase;
import bbejeck.model.RewardAccumulator;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Objects;


public class PurchaseRewardTransformer implements ValueTransformer<Purchase, RewardAccumulator> {

    private KeyValueStore<String, Integer> stateStore;
    private final String storeName;
    private ProcessorContext context;  /** 이 친구가 핵심인듯 */

    public PurchaseRewardTransformer(String storeName) {
        Objects.requireNonNull(storeName,"Store Name can't be null");
        this.storeName = storeName;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        stateStore = (KeyValueStore) this.context.getStateStore(storeName); /** storeName을 이용해 StateStore 인스턴스를 찾는다 */
    }

    /**
     * Purchase를 받아 RewardAccumulator로 변환 한다.
     * 상태를 이용
     * */
    @Override
    public RewardAccumulator transform(Purchase value) {
        RewardAccumulator rewardAccumulator = RewardAccumulator.builder(value).build(); /** [RewardAccumulator] 생성 -> Purchase를 이용해 현재 요청 상태 기준 */
        Integer accumulatedSoFar = stateStore.get(rewardAccumulator.getCustomerId()); /** 고객ID를 식별자로 누적 보상 포인트를 가져온다 */

        if (accumulatedSoFar != null) {
             rewardAccumulator.addRewardPoints(accumulatedSoFar); /** rewardAccumulator에 기존의 누적된 포인트를 더해준다 */
        }
        stateStore.put(rewardAccumulator.getCustomerId(), rewardAccumulator.getTotalRewardPoints());

        return rewardAccumulator;

    }

    @Override
    @SuppressWarnings("deprecation")
    public RewardAccumulator punctuate(long timestamp) {
        return null;  //no-op null values not forwarded.
    }

    @Override
    public void close() {
        //no-op
    }
}
