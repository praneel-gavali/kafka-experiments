package com.kafka.poc.elasticsearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
    private static final String BOOTSTRAP_SERVERS = "PLAINTEXT://192.168.29.74:9092";
    private static JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = createClient();

        KafkaConsumer<String, String> consumer = createConsumer("twitter_tweets");
        BulkRequest bulkRequest = new BulkRequest();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            logger.info("Received: {} records", records.count());
            for (ConsumerRecord<String, String> record : records) {
//Generic id:
//                String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                String value = record.value();
                try {
                    String id = extractIdFromTweet(value);
                    logger.info("Key: {}, Value {}", record.key(), value);
                    logger.info("Partition {} , Offset{}", record.partition(), record.offset());

                    //Insert record into elasticsearch
                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id)
                            .source(value, XContentType.JSON);

                    bulkRequest.add(indexRequest); // add it bulk request
//                IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
//                logger.info("id: {}", indexResponse.getId());
                } catch (NullPointerException e) {
                    logger.warn("Skipping bad data: {}", record.value());

                }
                if (records.count() > 0) {
                    BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                    logger.info("Commit offsets");
                    consumer.commitAsync();
                    logger.info("Offsets have been committed.");
                }
            }
        }

//        client.close();

    }

    private static String extractIdFromTweet(String tweet) {
        return jsonParser.parse(tweet).getAsJsonObject().get("id_str").getAsString();
    }

    private static RestHighLevelClient createClient() {
        String hostname = "kafka-experiment-1215738432.ap-southeast-2.bonsaisearch.net";
        String username = "6u1w3gagbd";
        String password = "96cltj41sh";

        //TODO: Research
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    private static KafkaConsumer<String, String> createConsumer(String topic) {
        String groupid = "kafka-elasticsearch";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupid);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); //Only ten records at time

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList(topic));

        return kafkaConsumer;
    }
}
