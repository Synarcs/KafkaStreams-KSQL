package Funds;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KGroupedTable;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;


public class Serialize {

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "assholeBitch");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();

        KTable<String,String> aggregated = builder.stream("asshole")
                .mapValues(values -> values.toString().toLowerCase())
                .flatMapValues((values) -> Arrays.asList(values.split(" ")))
                .selectKey((key,value) -> value)
                .groupByKey()
                .aggregate(
                        () -> "aggregated",
                        (String aggkey,String start,String end) -> start + "," + end,
                        Serdes.String(),
                        "aggregated_values_appended"
                );
        KGroupedTable<String,String> aggregatedGroup = aggregated.groupBy(
                        (key,value) -> new KeyValue<>(value,value),
                        Serdes.String(),
                        Serdes.String()
                );
        KStream<String,String>[] val = aggregated.toStream().branch(
                (String key,String value) -> value.split(" ")[0].length() == 6
        );

        aggregated.toStream().foreach((String key,String value) -> System.out.println("key is and value" + key + " " + value));
        aggregated.to("output");

        KafkaStreams stream = new KafkaStreams(builder,config);
        stream.cleanUp();
        stream.start();
        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));
    }
}
//        count("counts").to(Serdes.String(),Serdes.Long(),"output"); // modify wordcount
