package org.kafkajava.Streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
        import org.apache.kafka.streams.kstream.Printed;

import com.fasterxml.jackson.databind.JsonNode;
        import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class JsonTransformerStramsFunctions {

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("application.id", "jsonTransformerStramsFunctions");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());

        String input_topic="jsonTransfrom";
        AtomicInteger counter= new AtomicInteger();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> stream = builder.stream(input_topic);

        // map() example: extract the name field from the JSON message
        KStream<String, String> mappedStream = stream.map((key,value) -> {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = null;
            try {
                node = mapper.readTree(value);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            counter.getAndIncrement();
            System.out.println("Map() ::::: ");
            return new KeyValue<>(String.valueOf(counter.get()), node.get("name").asText());
        });

        // mapValues() example: convert the age field to an integer
        KStream<String, Long> mappedValuesStream = stream.mapValues(value -> {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = null;
            try {
                node = mapper.readTree(value);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            counter.getAndIncrement();
            System.out.println("mappedValuesStream() ::::: ");
            return node.get("age").asLong();
        });

        // filter() example: filter out users older than 65
        KStream<String, String> filteredStream = stream.filter((key, value) -> {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = null;
            try {
                node = mapper.readTree(value);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            System.out.println("filteredStream() ::::: ");
             return node.get("age").asLong() <= 65 ;
        });

        // flatMap() example: extract the occupation field and create a new message
        KStream<String, String> flatMappedStream = stream.flatMap((key, value) -> {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = null;
            try {
                node = mapper.readTree(value);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            String occupation = node.get("occupation").asText();
            System.out.println("flatMappedStream() ::::: ");
            return Arrays.asList(new KeyValue<>(key, "Occupation: " + occupation));
        });




        // foreach() example: print each message
        //stream.foreach((key, value) -> System.out.println("foreach() ::::  "+"Key: " + key + ", Value: " + value));

        // peek() example: print each message without modifying the stream
        //stream.peek((key, value) -> System.out.println("peek() ::::  "+"Key: " + key + ", Value: " + value));


        // print() example: print the stream to the console
        //stream.print(Printed.toSysOut());

        //Printing Output
      //  mappedStream.print(Printed.toSysOut());
       // mappedValuesStream.print(Printed.toSysOut());
      //  filteredStream.print(Printed.toSysOut());
      //  flatMappedStream.print(Printed.toSysOut());



        KafkaStreams streams = new KafkaStreams(builder.build(), props);


        streams.start();

        Thread.sleep(30000);
        streams.close();
    }
}