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

package bbejeck.chapter_4;

import bbejeck.chapter_4.joiner.PurchaseJoiner;
import bbejeck.chapter_4.timestamp_extractor.TransactionTimestampExtractor;
import bbejeck.clients.producer.MockDataProducer;
import bbejeck.model.CorrelatedPurchase;
import bbejeck.model.Purchase;
import bbejeck.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


@SuppressWarnings("unchecked")
public class KafkaStreamsJoinsApp {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsJoinsApp.class);

    public static void main(String[] args) throws Exception {

        StreamsConfig streamsConfig = new StreamsConfig(getProperties());
        StreamsBuilder builder = new StreamsBuilder();


        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<String> stringSerde = Serdes.String();

        KeyValueMapper<String, Purchase, KeyValue<String,Purchase>> custIdCCMasking = (k, v) -> {
            Purchase masked = Purchase.builder(v).maskCreditCard().build();
            return new KeyValue<>(masked.getCustomerId(), masked);
        };

        /** {@link ZMartKafkaStreamsAdvancedReqsApp} 92라인 참고 */
        Predicate<String, Purchase> coffeePurchase = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("coffee");
        Predicate<String, Purchase> electronicPurchase = (key, purchase) -> purchase.getDepartment().equalsIgnoreCase("electronics");

        int COFFEE_PURCHASE = 0;
        int ELECTRONICS_PURCHASE = 1;

        KStream<String, Purchase> transactionStream = builder.stream( "transactions", Consumed.with(Serdes.String(), purchaseSerde)).map(custIdCCMasking); /** selectKey 처리 노드를 삽입 */

        /**
         *  새로운 key를 생성하는 메소드 (selectKey, map, transform)을 호출하여 내부 bool flag를 true로 변경한다 -> 리파티셔닝이 필요하다고 설정된다.
         *  이 flag를 사용해 조인, 리듀스, 집계 연산을 수행하면 자동으로 리파티셔닝 처리 된다.
         *  분기 작업을 즉시 수행하기 위해 branch() 호출로 인한 각 KStream에도 리파티셔닝 플래그가 지정된다.
         *
         *  selectKey()는 key를 수정한다. -> 리파티셔닝이 진행된다.
         */
        KStream<String, Purchase>[] branchesStream = transactionStream.selectKey((k,v)-> v.getCustomerId()).branch(coffeePurchase, electronicPurchase);

        KStream<String, Purchase> coffeeStream = branchesStream[COFFEE_PURCHASE]; /** 분기 처리된 stream */
        KStream<String, Purchase> electronicsStream = branchesStream[ELECTRONICS_PURCHASE];

        ValueJoiner<Purchase, Purchase, CorrelatedPurchase> purchaseJoiner = new PurchaseJoiner(); /** 조인을 수행하는 ValueJoiner 인스턴스 */
        JoinWindows twentyMinuteWindow =  JoinWindows.of(60 * 1000 * 20); /** 타임 스탬프로 20분 이내면 조인 발생 */

        /** 내부 join -> 두 record가 존재하지 않을 경우 join이 발생하지 않는다.
         * join()을 통해 coffeeStream과 electronicsStream의 자동 리파티셔닝을 작동시킨다.
         */
        KStream<String, CorrelatedPurchase> joinedKStream = coffeeStream.join(electronicsStream, /** coffeeStream에 electronicsStream을 join 처리한다. 조인 할 가전 구매 스트림 */
                                                                              purchaseJoiner, /** ValueJoiner */
                                                                              twentyMinuteWindow, /** [JoinWindows] 인스턴스, 조인에 포함 될 두 값 사이의 최대 시간차이. 순서는 포함되지 않는다. */
                                                                              Joined.with(stringSerde, /** 조인을 수행하기 위한 선택적 매개변수 */
                                                                                          purchaseSerde,
                                                                                          purchaseSerde));

        joinedKStream.print(Printed.<String, CorrelatedPurchase>toSysOut().withLabel("joined KStream"));

        // used only to produce data for this application, not typical usage
        MockDataProducer.producePurchaseData();

        LOG.info("Starting Join Examples");
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        kafkaStreams.start();
        Thread.sleep(65000);
        LOG.info("Shutting down the Join Examples now");
        kafkaStreams.close();
        MockDataProducer.shutdown();


    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "join_driver_application");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "join_driver_group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "join_driver_client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, TransactionTimestampExtractor.class); /** kafka timestamp가 아닌 실제 거래 timestamp를 사용하도록 설정한다. */
        return props;
    }


}
