package bbejeck.chapter_5;


import bbejeck.clients.producer.MockDataProducer;
import bbejeck.model.StockTickerData;
import bbejeck.util.serde.StreamsSerdes;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import static bbejeck.clients.producer.MockDataProducer.STOCK_TICKER_STREAM_TOPIC;
import static bbejeck.clients.producer.MockDataProducer.STOCK_TICKER_TABLE_TOPIC;

public class KStreamVsKTableExample {

    private static final Logger LOG = LoggerFactory.getLogger(KStreamVsKTableExample.class);

    public static void main(String[] args) throws Exception {

        StreamsConfig streamsConfig = new StreamsConfig(getProperties());

        StreamsBuilder builder = new StreamsBuilder(); /** KTable, KStream 모두 StreamsBuilder를 이용해 생성한다. */


        /** KTable 인스턴스 생성
         * 인스턴스를 생성하고 동시에 내부에 스트림 상태를 추적하는 상태 저장소(State Store)를 만들어 업데이트 스트림을 만든다.
         * 저장소는 내부적인 이름을 갖는다 -> 대화형 쿼리에 이용 불가능하다.
         */
        KTable<String, StockTickerData> stockTickerTable = builder.table(STOCK_TICKER_TABLE_TOPIC);
        KStream<String, StockTickerData> stockTickerStream = builder.stream(STOCK_TICKER_STREAM_TOPIC); /** KStream 인스턴스 생성 */

        stockTickerTable.toStream().print(Printed.<String, StockTickerData>toSysOut().withLabel("Stocks-KTable")); /** KTable이 콘솔에 결과 출력 */
        stockTickerStream.print(Printed.<String, StockTickerData>toSysOut().withLabel("Stocks-KStream")); /** KStream 콘솔에 결과 출력 */

        int numberCompanies = 3;
        int iterations = 3;

        MockDataProducer.produceStockTickerData(numberCompanies, iterations);

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), streamsConfig);
        LOG.info("KTable vs KStream output started");
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Thread.sleep(15000);
        LOG.info("Shutting down KTable vs KStream Application now");
        kafkaStreams.close();
        MockDataProducer.shutdown();

    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "KStreamVSKTable_app");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KStreamVSKTable_group");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "KStreamVSKTable_client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "30000"); /** 프로세서 상태 저장(commit) 주기. 캐시를 비우고 마지막 업데이트 레코드를 다운스트림에 전송 */
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "15000");
        //props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0"); /** KTable의 캐시 버퍼 사이즈. 0=사용 안 함 */
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1"); /** 스트림 스레드 수 */
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName()); /** 기본 key serd 등록 */
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StreamsSerdes.StockTickerSerde().getClass().getName()); /** 기본 value serd 등록 */
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;

    }
}
