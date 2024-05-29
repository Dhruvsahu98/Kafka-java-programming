package org.kafkajava.Streams;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

public class KafkaStreamsProducerConsumer {

    public static void main(String[] args) {

        String sourceTopic = "demo-java-suhas1";
        String targetTopic = "demo-java-suhas2";

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> sourceStream = builder.stream(sourceTopic);

        sourceStream.to(targetTopic);

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, KafkaStreamsConfig.getStreamsConfig());

        streams.start();
    }
}