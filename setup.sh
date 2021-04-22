
echo "console-consumer"
bash bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic streamFunding --from-beginning --property print.key=true --property print.value=true --property key.deserialization=org.apache.kafka.common.serialization.StringDeserializer --property value.deserialization=org.apache.kafka.common.serialization.StringDeserialization
echo "console-aggregated-consumer"
bash bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic aggregateFunds --from-beginning --property print.key=true --property print.value=true --property key.deserialization=org.apache.kafka.common.serialization.StringDeserializer --property value.deserialization=org.apache.kafka.common.serialization.StringDeserializatio

