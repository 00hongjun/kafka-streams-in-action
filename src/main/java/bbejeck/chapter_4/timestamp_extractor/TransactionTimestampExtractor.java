package bbejeck.chapter_4.timestamp_extractor;

import bbejeck.model.Purchase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;


public class TransactionTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
        Purchase purchasePurchaseTransaction = (Purchase) record.value(); /** 카프카에 보내진 key/value 쌍에서 Purchase 객체를 찾음 */
        return purchasePurchaseTransaction.getPurchaseDate().getTime(); /** 구매 시점에 기록된 타임스탬프를 반환 */
    }
}
