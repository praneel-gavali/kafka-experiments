package com.kafka.poc.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    public static final String TOPIC = "twitter_tweets";
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    private static final String consumerKey = "";
    private static final String consumerSecret = "";
    private static final String token = "";
    private static final String tokenSecret = "";
    private static final String BOOTSTRAP_SERVERS = "PLAINTEXT://192.168.29.74:9092";

    List<String> terms = Lists.newArrayList("India");

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

        //Create twitter client
        Client client = createTwitterClient(msgQueue);
        client.connect();

        //Create kafka producer
        KafkaProducer<String, String> producer = createKafkaProducer();

        //Loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("Error wile poll", e);
                client.stop();
            }
            if (msg != null) {
                logger.info("Twitter Message {}", msg);
                producer.send(new ProducerRecord<>(TOPIC, null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            logger.error("Error while publishing record to topic", exception);
                        }
                    }
                });
            }

        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down Twitter application");
            client.stop();
            producer.close();
        }));
        logger.info("End of application");
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {


        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        // Optional: set up some followings and track terms
        List<Long> followings = Lists.newArrayList(1234L, 566788L);

        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
//                .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();

        // Attempts to establish a connection.
        return hosebirdClient;
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        //Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(5));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));

        //Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        return producer;
    }
}
