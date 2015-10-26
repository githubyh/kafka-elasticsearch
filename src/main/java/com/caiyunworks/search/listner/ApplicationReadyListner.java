package com.caiyunworks.search.listner;

import com.caiyunworks.search.kafka.ConsumerTask;
import kafka.javaapi.consumer.ConsumerConnector;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created on 10/24/15.
 *
 * @author Tony Wang (ziscloud@gmail.com)
 */
@Component
public class ApplicationReadyListner implements ApplicationListener<ContextRefreshedEvent>, DisposableBean {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());
    private ExecutorService executorService;

    @Autowired
    private ConsumerConnector kafkaConsumer;
    @Autowired
    private Client esClient;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent contextRefreshedEvent) {
        executorService = Executors.newFixedThreadPool(1);
        executorService.submit(new ConsumerTask(kafkaConsumer, esClient));
    }

    @Override
    public void destroy() throws Exception {
        kafkaConsumer.shutdown();
        executorService.shutdown();
    }
}
