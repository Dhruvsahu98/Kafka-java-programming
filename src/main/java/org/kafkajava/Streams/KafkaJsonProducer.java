package org.kafkajava.Streams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class KafkaJsonProducer {

    public static void main(String[] args) {

        // Set up Kafka producer properties
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // replace with your Kafka broker URL
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        String input_topic = "jsonTransfrom";

        // Create a Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        Random random = new Random();

        while (true) {
            // Generate random data
            String name = "User " + random.nextInt(100);
            int age = random.nextInt(100);
            String occupation = "Occupation " + random.nextInt(10);

            // Define the JSON schema to publish
            String jsonSchema = "{\"name\":\"" + name + "\",\"age\":" + age + ",\"occupation\":\"" + occupation + "\"}";

            // Create a ProducerRecord with the JSON schema
            ProducerRecord<String, String> record = new ProducerRecord<>(input_topic, jsonSchema);

            // Publish the JSON schema to the topic
            producer.send(record);

            // Print a message to indicate that a message has been sent
            System.out.println("Sent message: " + jsonSchema);

            // Wait for 1 second before sending the next message
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}