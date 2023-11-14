package org.kafka.demo;


import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    public static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Hello I'm a Consumer...!");

        String topic1 = "first_topic";
        String topic2 = "second_topic";

        Properties props = new Properties();
        props.put("bootstrap.servers", "mighty-peacock-7444-eu2-kafka.upstash.io:9092");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"bWlnaHR5LXBlYWNvY2stNzQ0NCQhSnTe3CBzHIGCFwA7MLwPz49t3DN2wY0HBfo\" password=\"MDljZWIyYTgtNDUyOS00MGZkLThmZDQtOTI0MDFiMDU2M2U0\";");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");
        props.put("group.id", "my_consumer");

//        Partitions rebalanced strategy. By default it is eager rebalanced
//        props.put("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected SHUTDOWN..!!!");
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try{
            //subscribe to a topic
            consumer.subscribe(Arrays.asList(topic1, topic2));

            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : consumerRecords) {
                    log.info("Topic: " + record.topic() + " | Partition: " + record.partition()
                            + " | Key: " + record.key() + " | Message: " + record.value());
                }
            }
        }catch (WakeupException e){
            log.info("Consumer is starting to shutdown...!!!!");
        }catch (Exception e){
            log.error("Exception Occurred: ", e);
        }finally {
            consumer.close();
            log.info("SHUT DOWN !!!!!!!!!!!!!!!!!!");
        }

    }
}
