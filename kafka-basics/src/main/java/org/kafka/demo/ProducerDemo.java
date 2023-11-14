package org.kafka.demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    public static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello I'm a Producer...!");
        Properties props = new Properties();
        props.put("bootstrap.servers", "mighty-peacock-7444-eu2-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"bWlnaHR5LXBlYWNvY2stNzQ0NCQhSnTe3CBzHIGCFwA7MLwPz49t3DN2wY0HBfo\" password=\"MDljZWIyYTgtNDUyOS00MGZkLThmZDQtOTI0MDFiMDU2M2U0\";");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("second_topic", "hey! here is my second topic message");
        producer.send(producerRecord);
        producer.flush();
        producer.close();

    }
}
