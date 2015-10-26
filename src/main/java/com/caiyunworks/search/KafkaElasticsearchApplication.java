package com.caiyunworks.search;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

@Configuration
@ComponentScan
public class KafkaElasticsearchApplication {

    @Bean
    public ConsumerConnector initKafkaConsumer() {
        Properties props = new Properties();
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "canal");
        props.put("zookeeper.session.timeout.ms", "10000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        return Consumer.createJavaConsumerConnector(consumerConfig);
    }

    @Bean
    public Client initElasticSearchClient() {
        Client client = new TransportClient()
                .addTransportAddress(new InetSocketTransportAddress("localhost", 9300));
        return client;
    }

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(KafkaElasticsearchApplication.class);
        app.setShowBanner(false);
        app.setWebEnvironment(false);
        app.run(args);
    }
}
