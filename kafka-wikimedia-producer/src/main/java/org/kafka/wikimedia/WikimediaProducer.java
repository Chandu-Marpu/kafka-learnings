package org.kafka.wikimedia;

import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
//        props.put("sasl.mechanism", "SCRAM-SHA-256");
//        props.put("security.protocol", "SASL_SSL");
//        props.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"bWlnaHR5LXBlYWNvY2stNzQ0NCQhSnTe3CBzHIGCFwA7MLwPz49t3DN2wY0HBfo\" password=\"MDljZWIyYTgtNDUyOS00MGZkLThmZDQtOTI0MDFiMDU2M2U0\";");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        String topic = "wikimedia.recentchange";

        WikimediaHandler eventHandler = new WikimediaHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource source = builder.build();
        source.start();

        TimeUnit.MINUTES.sleep(1);
    }
}
