package org.kafkajava.Streams;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Pattern;

public class KafkaStreamsDemo {
    private static final Logger log = LoggerFactory.getLogger(KafkaStreamsDemo.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        log.info("I am a Kafka Producer");

        String bootstrapServers = "127.0.0.1:9092";
        String applicationid = "wordcount-live-test";
        String inputtopic= "inputTopic";
        //String outputTopic = "outputTopic";

        //Creating temprory directory for managing state
        Path stateDirectory = Files.createTempDirectory("kafka-streams");

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,applicationid);
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        //String Serializers and Deserializers
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass().getName());
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG,stateDirectory.toAbsolutePath().toString());

        //Building Streaming Topology / Data Transformation

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream(inputtopic);
        Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);

        KTable<String, Long> wordCounts = textLines
                .flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase())))
                .groupBy((key, word) -> word)
                .count();

        // Handling Results/ Handling Output
        wordCounts.toStream()
                .foreach((word, count) -> System.out.println("word: " + word + " -> " + count));

//       Case study scenerio
//        String outputTopic = "outputTopic";
//        wordCounts.toStream()
//                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        //Starting KafkaStream Job  (Invoking Streams / Starting streams)
        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, properties);
        streams.start();

        Thread.sleep(30000);
        streams.close();
    }
}