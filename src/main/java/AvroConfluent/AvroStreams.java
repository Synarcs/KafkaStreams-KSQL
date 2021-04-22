package AvroConfluent;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.avro.generic.GenericRecord;

import java.util.Arrays;
import java.util.Properties;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

public class AvroStreams {
    final static String schema_url = "http://localhost:8081";
    public static Properties generateConfig(){
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,KafkaAvroDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,schema_url);
        return props;
    }

    public static KStreamBuilder buildTopology(){
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.put("broadcast",currentIpConfigure.newBuilder().getNetmask()
                .toString());
        node.put("inet",currentIpConfigure.newBuilder().getInet().toString());
        KStreamBuilder builder = new KStreamBuilder();
        KGroupedStream<String,String> streams = builder.<String,currentIpConfigure>stream("avroSerde")
                .map((key,value) -> new KeyValue<>(value.getNetmask().toString(),value.getInet()))
                .mapValues((values) -> values.toString().toLowerCase())
                .groupByKey();

        streams.count("ip_count_store")
                .to("total_count");
        return builder;
    }

    public SpecificAvroSerde<currentIpConfigure> configure(){
        HashMap map = new HashMap();
        map.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                schema_url);
        SpecificAvroSerde<currentIpConfigure> config = new SpecificAvroSerde<>();
        config.configure(map,false);
        return config;
    }

    public static void main(String[] args) {
        final Consumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(generateConfig());
        consumer.subscribe(Arrays.asList("avroSerde"));

        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(1000);
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s \n", record.offset(), record.key(), record.value().toString());
                }
            }
        } finally {
            final CountDownLatch latch = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread("shutdown"){
                @Override
                public void run(){
                    consumer.close();
                    latch.countDown();
                }
            });
        }

    }
}
//    public static KafkaStreams createTopology(){
//        KStreamBuilder builder = new KStreamBuilder();
//        KStream<String,currentIpConfigure> valies  = builder.stream("avroSerde");
////        publish  the info for the bytes transfered to the kafka brocker
//        valies.peek((key,value) -> {
//            System.out.println("the key is " + key);
//            System.out.println("the id sent to brocker" + value.toString());
//        });
//        KafkaStreams streams = new KafkaStreams(builder,generateConfig());
//        return streams;
//    }

