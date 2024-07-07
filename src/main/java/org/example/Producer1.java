package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Producer1 {

    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    private final static String TOPIC_NAME= "topic5";

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.ACKS_CONFIG,"all");
        configs.put(ProducerConfig.RETRIES_CONFIG,3);

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        String message = "First message";

        ProducerRecord<String,String> record = new ProducerRecord<>(TOPIC_NAME,message);

        Future<RecordMetadata> send = producer.send(record);

        RecordMetadata recordMetadata = send.get();

        System.out.println("message = " + message);
        System.out.println("recordMetadata.partition() = " + recordMetadata.partition());
        System.out.println("recordMetadata.offset() = " + recordMetadata.offset());
        producer.flush();
        producer.close();
    }
}
