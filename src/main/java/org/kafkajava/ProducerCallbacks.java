package org.kafkajava;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
public class ProducerCallbacks  {
    private static final Logger log = LoggerFactory.getLogger(ProducerCallbacks.class);

    public static void main(String[] args) {
        log.info("I am a Kafka Producer");

        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"400");
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

      for(int j=0;j<10;j++) {
          for (int i = 0; i < 400; i++) {
              // create a producer record
              ProducerRecord<String, String> producerRecord =
                      new ProducerRecord<String, String>("demo-java-callback-5" ,"hello world " + Integer.toString(i));

              // send data - asynchronous
              producer.send(producerRecord, new Callback() {
                  public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                      // executes every time a record is successfully sent or an exception is thrown
                      if (e == null) {
                          // the record was successfully sent
                          log.info(
                                  "Partition: " + recordMetadata.partition() + "|" +
                                  "Offset: " + recordMetadata.offset() + "|" + "record : "+ producerRecord.value());
                      } else {
                          log.error("Error while producing", e);
                      }
                  }
              });



          }

          try {
              Thread.sleep(1000);
          } catch (InterruptedException e) {
              e.printStackTrace();
          }
      }
        // flush data - synchronous
        producer.flush();
        // flush and close producer
        producer.close();
    }
}
