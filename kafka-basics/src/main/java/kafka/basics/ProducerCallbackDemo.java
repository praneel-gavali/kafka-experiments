package kafka.basics;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerCallbackDemo {


    public static final String BOOTSTRAP_SERVERS = "PLAINTEXT://192.168.29.74:9092";

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerCallbackDemo.class.getName());

        //Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("test-topic", "hello_world");

        producer.send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
                logger.info("Received Metadata. \n Topic: {} \n Partition: {} \n Offsets: {} \n Timestamp: {}"
                        , metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp());
            } else {
                logger.error("Error while producing", exception);
            }
        });
        producer.flush();
        producer.close();
    }
}
