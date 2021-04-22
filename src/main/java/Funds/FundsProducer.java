package Funds;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.Properties;

public class FundsProducer {
    final String ProducerApplicationName = "Idempotent only once Record Producer Record";

    public static Properties setProps(){
        Properties pr = generateProperties();
        pr.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        pr.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        pr.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        pr.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        pr.setProperty(ProducerConfig.RETRIES_CONFIG,"4");
        pr.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");return pr;
    }
    public static Properties generateProperties(){
        Properties pr = new Properties();
        return pr;
    }
    public static ProducerRecord<String, String>  createProducerRecord(String name, int min, int max){
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();
        Integer transactionAmount = ThreadLocalRandom.current().nextInt(min,max);

        transaction.put("name",name);
        transaction.put("amount",transactionAmount);
        transaction.put("time",Instant.now().toString());
        return new ProducerRecord<String,String>("streamFunding",name,transaction.toString());
    }
    public static void main(String[] args) {
        KafkaProducer<String,String > prod = new KafkaProducer<String, String>(setProps());
        String[] user = new String[]{"daniel","kevin","bryane"};
        final int min = 0;
        final int max = 500;
        int ProducerBatchSizerRecord = 0;
        while (true){
            try {
                System.out.println("running on the batch file"+ ProducerBatchSizerRecord);
//            100 records per sec
                prod.send(createProducerRecord(user[ProducerBatchSizerRecord % user.length],min,max));
                Thread.sleep(60);
                prod.send(createProducerRecord(user[ProducerBatchSizerRecord % user.length], min, max), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e == null){
                            System.out.println(recordMetadata.offset() + recordMetadata.partition() + recordMetadata.topic());
                        }
//                            System.out.println(recordMetadata.partition()); // for testing purpose
                    }
                });
                Thread.sleep(60);
                prod.send(createProducerRecord(user[ProducerBatchSizerRecord % user.length],min,max));
                ProducerBatchSizerRecord++;
            }catch (InterruptedException exp){
                break;
            }
        }
        prod.close();
        prod.flush();
        System.out.println("record is written to the streams in batch producing");
    }
}
