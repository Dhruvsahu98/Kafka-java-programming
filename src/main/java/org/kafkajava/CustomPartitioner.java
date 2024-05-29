package org.kafkajava;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class CustomPartitioner implements Partitioner {

    private static final Logger log = LoggerFactory.getLogger(CustomPartitioner.class);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        String truckid = extractTruckIdType(key.toString()); //key.toString() = "id_2" // truckid = "2"
        return Integer.valueOf(truckid); //2
    }

    @Override
    public void close() {

    }

    public String extractTruckIdType(String key){
        return key.substring(key.indexOf('_')+1);
    }


    @Override
    public void configure(Map<String, ?> map) {

    }
}