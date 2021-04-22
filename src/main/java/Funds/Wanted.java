package Funds;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class Wanted {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "colors");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        ArrayList<String> colorsValid = new ArrayList<>();colorsValid.add("red");colorsValid.add("green");colorsValid.add("blue");
        colorsValid.add("black");
        KStreamBuilder builder = new KStreamBuilder();
        KStream<String,String> colors =  builder.stream("usercolors")
                .mapValues(values -> values.toString().toLowerCase())
                .filter((key,value) -> value.contains(","))
                .selectKey((key,value) -> value.split(",")[0].toLowerCase())
                .mapValues((values) -> values.split(",")[1].toLowerCase())
                .filter((key,values) -> colorsValid.contains(values));
        colors.foreach((key,value) -> System.out.println(key + value));
        colors.to(Serdes.String(),Serdes.String(),"intermidiatetopic");
        KTable<String,String> table = builder.table("intermidiatetopic");
        KTable<String,Long> writer = table.groupBy((key,value) -> new KeyValue<>(value,value))
                .count("color_count");
        writer.toStream().peek((key,value) -> System.out.println("after peek in" + key + value));
        writer.to(Serdes.String(),Serdes.Long(),"sameColorUsers");
        KafkaStreams streams = new KafkaStreams(builder,config);
        streams.cleanUp();
        streams.start();
        System.out.println(streams.toString());
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
