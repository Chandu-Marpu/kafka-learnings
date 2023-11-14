package org.kafka.demo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerCallbackDemo {
    public static final Logger log = LoggerFactory.getLogger(ProducerCallbackDemo.class);

    public static void main(String[] args) {
        log.info("Producer Callbacks: Confirm the partition & offset the message was sent...!");
        Properties props = new Properties();
        props.put("bootstrap.servers", "mighty-peacock-7444-eu2-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"bWlnaHR5LXBlYWNvY2stNzQ0NCQhSnTe3CBzHIGCFwA7MLwPz49t3DN2wY0HBfo\" password=\"MDljZWIyYTgtNDUyOS00MGZkLThmZDQtOTI0MDFiMDU2M2U0\";");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "hey! here is my first message");
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //this will run each time a message sent or an exception occurred
                if (e == null) {
                    log.info("Received new Metadata: \n " +
                            "Topic : " + recordMetadata.topic() + "\n " +
                            "Partition: " + recordMetadata.partition() + "\n " +
                            "Offset: " + recordMetadata.offset());
                } else {
                    log.error("Error : ", e);
                }
            }
        });
        producer.flush();
        producer.close();
    }
}
