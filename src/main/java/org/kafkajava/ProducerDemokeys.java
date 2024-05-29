package org.kafkajava;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemokeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemokeys.class);

    public static void main(String[] args) {
        log.info("I am a Kafka Producer");

        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);



        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {

                // create a producer record

                String topic = "demo-java-key-1";
                String value = "hello world " + Integer.toString(i);
                String key = "id_" + Integer.toString(i);


                ProducerRecord<String, String> producerRecordwithkeys =
                        new ProducerRecord<>(topic, key, value);
                // send data - asynchronous
                producer.send(producerRecordwithkeys, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // executes every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            // the record was successfully sent
                            log.info("Key:" + key + "|"
                                    + "Partition: " + recordMetadata.partition() + "|"+ value +"\n");
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });


            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        // flush data - synchronous
        producer.flush();
        // flush and close producer
        producer.close();
    }
}