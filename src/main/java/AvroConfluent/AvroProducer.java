package AvroConfluent;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Properties;

public class AvroProducer {
    public static Properties props(){
        Properties pr = generateProperties();
        String acksAll = AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;
        pr.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        pr.setProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,"http://localhost:8081");
        pr.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        pr.setProperty(ProducerConfig.RETRIES_CONFIG,"4");
        pr.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        pr.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        pr.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");return pr;
    }

    public static Properties generateProperties(){
        Properties pr = new Properties();
        return pr;
    }

    public static ProducerRecord<String,currentIpConfigure> generateRecord(){
        currentIpConfigure configure = currentIpConfigure.newBuilder()
                .setInet("127.0.0."+ ThreadLocalRandom.current().nextInt(1,255))
                .setActiveStatus(true)
                .setBroadcast("192.168.1.255")
                .setNetmask("255.255.255.0")
                .setPacketsRecieved(ThreadLocalRandom.current().nextInt(0,1400))
                .build();
        ProducerRecord<String,currentIpConfigure> record = new ProducerRecord<String,currentIpConfigure>("avroSerde",
                Integer.toString(ThreadLocalRandom.current().nextInt(0,3)),
                configure
        );
        return record;
    }

    public static void  shortestRoute(){}

    public static void main(String[] args) {
        KafkaProducer<String,currentIpConfigure> producer = new KafkaProducer<String,currentIpConfigure>(props());

        int endThreadCount = 0;
        Thread starter = new Thread("starter");
        while (endThreadCount <= 1500){
            ProducerRecord<String,currentIpConfigure> record = generateRecord();
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        System.out.println("data topic Details");
                        System.out.println(recordMetadata.offset() + " " + recordMetadata.partition() + recordMetadata.topic());
                    }
                }
            });
            endThreadCount++;
        }
        CountDownLatch latch = new CountDownLatch(1);

        producer.flush();
        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
    }
}
