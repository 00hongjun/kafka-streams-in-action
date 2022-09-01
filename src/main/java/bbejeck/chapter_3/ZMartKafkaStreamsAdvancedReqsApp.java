/*
 * Copyright 2016 Bill Bejeck
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package bbejeck.chapter_3;

import bbejeck.chapter_3.service.SecurityDBService;
import bbejeck.clients.producer.MockDataProducer;
import bbejeck.model.Purchase;
import bbejeck.model.PurchasePattern;
import bbejeck.model.RewardAccumulator;
import bbejeck.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

@SuppressWarnings("unchecked")
public class ZMartKafkaStreamsAdvancedReqsApp {

    private static final Logger LOG = LoggerFactory.getLogger(ZMartKafkaStreamsAdvancedReqsApp.class);

    public static void main(String[] args) throws Exception {

        StreamsConfig streamsConfig = new StreamsConfig(getProperties());

        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde(); /** 저자는 [StreamsSerdes] 유틸을 만들고 그 안에 Serd들을 선언했다. */
        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();
        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder(); /** 이 builder를 이용해 [KafkaStreams]을 생성한다 */


        // previous requirements
        KStream<String,Purchase> purchaseKStream = builder.stream( "transactions", Consumed.with(stringSerde, purchaseSerde)) /** [purchaseKStream]이 아래에서 사용할 source 이다 */
                .mapValues(p -> Purchase.builder(p).maskCreditCard().build()); // value를 읽고 [Purchase]를 생성한다

        KStream<String, PurchasePattern> patternKStream = purchaseKStream.mapValues(purchase -> PurchasePattern.builder(purchase).build()); /** 위에서 생성한 [purchaseKStream]를 이용하여 [PurchasePattern]을 생성 */

        patternKStream.print( Printed.<String, PurchasePattern>toSysOut().withLabel("patterns"));
        patternKStream.to("patterns", Produced.with(stringSerde,purchasePatternSerde));


        KStream<String, RewardAccumulator> rewardsKStream = purchaseKStream.mapValues(purchase -> RewardAccumulator.builder(purchase).build());

        rewardsKStream.print(Printed.<String, RewardAccumulator>toSysOut().withLabel("rewards")); /** [KStream.print] -> stout 출력*/
        rewardsKStream.to("rewards", Produced.with(stringSerde,rewardAccumulatorSerde)); /** [rewards] 토픽에 쓴다 */



           // selecting a key for storage and filtering out low dollar purchases


        KeyValueMapper<String, Purchase, Long> purchaseDateAsKey = (key, purchase) -> purchase.getPurchaseDate().getTime(); /** Long 타입 구매 날짜를 key로 변환하는 KeyValueMapper */

        KStream<Long, Purchase> filteredKStream = purchaseKStream.filter((key, purchase) -> purchase.getPrice() > 5.00).selectKey(purchaseDateAsKey); /** 5달러 미만의 구매 필터링, 위에서 생성한 KeyValueMapper를 이용한다. -> 새로운 key 생성 */

        filteredKStream.print(Printed.<Long, Purchase>toSysOut().withLabel("purchases"));
        filteredKStream.to("purchases", Produced.with(Serdes.Long(),purchaseSerde)); /** key type이 변경 됐으므로 serd도 변경 된다 */



         // branching stream for separating out purchases in new departments to their own topics

        Predicate<String, Purchase> isCoffee = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("coffee"); /** 이 [Predicate]를 이용해 스트림을 분리할 수 있다. */
        Predicate<String, Purchase> isElectronics = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("electronics");

        int coffee = 0;  /** [KStream.branch] 의 예상 index값이다. 아래 코드에서 [.branch(isCoffee, isElectronics)]으로 0, 1 순서로 처리했다 */
        int electronics = 1;

        KStream<String, Purchase>[] kstreamByDept = purchaseKStream.branch(isCoffee, isElectronics);
        /** [KStream.branch]로 스트림을 분리 한다.
         * 커피, 전자제품으로 분리
         * 배열은 인자로 받은 predicate의 순으로 배정되며, filter에 걸리지 않은 레코드는 삭제된다.
         */


        kstreamByDept[coffee].to( "coffee", Produced.with(stringSerde, purchaseSerde)); /** coffee 토픽에 쓰기 */
        kstreamByDept[coffee].print(Printed.<String, Purchase>toSysOut().withLabel( "coffee"));

        kstreamByDept[electronics].to("electronics", Produced.with(stringSerde, purchaseSerde));
        kstreamByDept[electronics].print(Printed.<String, Purchase>toSysOut().withLabel("electronics"));




         // security Requirements to record transactions for certain employee
        ForeachAction<String, Purchase> purchaseForeachAction = (key, purchase) ->
                SecurityDBService.saveRecord(purchase.getPurchaseDate(), purchase.getEmployeeId(), purchase.getItemPurchased()); /** [ForeachAction<K, V>]을 정의 한다 */

        
        purchaseKStream.filter((key, purchase) -> purchase.getEmployeeId().equals("000000")).foreach(purchaseForeachAction);


        // used only to produce data for this application, not typical usage
        MockDataProducer.producePurchaseData();
        
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),streamsConfig);
        LOG.info("ZMart Advanced Requirements Kafka Streams Application Started");
        kafkaStreams.start();
        Thread.sleep(65000);
        LOG.info("Shutting down the Kafka Streams Application now");
        kafkaStreams.close();
        MockDataProducer.shutdown();
    }




    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "Example-Kafka-Streams-Job");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "streams-purchases");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "testing-streams-api");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }

}
