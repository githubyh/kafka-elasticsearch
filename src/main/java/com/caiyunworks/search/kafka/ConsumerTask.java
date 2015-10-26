package com.caiyunworks.search.kafka;

import com.alibaba.otter.canal.protocol.Message;
import com.google.gson.Gson;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.otter.canal.protocol.CanalEntry.*;

/**
 * Created on 10/24/15.
 *
 * @author Tony Wang (ziscloud@gmail.com)
 */
public class ConsumerTask implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private ConsumerConnector kafkaConsumer;
    private Client esClient;
    private Gson gson;

    public ConsumerTask(ConsumerConnector kafkaConsumer, Client esClient) {
        this.kafkaConsumer = kafkaConsumer;
        this.esClient = esClient;
        gson = new Gson();
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put("canal", new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = kafkaConsumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get("canal").get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            String msg = new String(it.next().message());
            if (logger.isDebugEnabled()) {
                logger.debug("got the canal message: {}", msg);
            }
            try {
                Message message = gson.fromJson(msg, Message.class);
                List<Entry> entries = message.getEntries();
                for (Entry entry : entries) {
                    if (entry.getEntryType() == EntryType.ROWDATA) {
                        RowChange rowChage = null;
                        try {
                            rowChage = RowChange.parseFrom(entry.getStoreValue());
                        } catch (Exception e) {
                            throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                        }
                        Header header = entry.getHeader();
                        String schemaName = header.getSchemaName();
                        String tableName = header.getTableName();

                        EventType eventType = rowChage.getEventType();

                        for (RowData rowData : rowChage.getRowDatasList()) {
                            if (eventType == EventType.DELETE) {
                                Map<String, Object> record = parseColumns(rowData.getBeforeColumnsList());
                                DeleteResponse deleteResponse = esClient.prepareDelete(schemaName, tableName, record.get("id").toString())
                                        .execute()
                                        .actionGet();
                                logger.info("delete index successful, response is: {}", deleteResponse);
                            } else if (eventType == EventType.INSERT) {
                                Map<String, Object> record = parseColumns(rowData.getAfterColumnsList());
                                IndexResponse indexResponse = esClient.prepareIndex(schemaName, tableName, record.get("id").toString())
                                        .setSource(record)
                                        .execute()
                                        .actionGet();
                                logger.info("create index successful, response is: {}", indexResponse);
                            } else if (eventType == EventType.UPDATE) {
                                Map<String, Object> record = parseColumns(rowData.getAfterColumnsList());
                                UpdateResponse updateResponse = esClient.prepareUpdate(schemaName, tableName, record.get("id").toString())
                                        .setDoc(record)
                                        .execute()
                                        .actionGet();
                                logger.info("update index successful, response is: {}", updateResponse);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("process canal message failed.", e);
            }
        }
    }

    private Map<String, Object> parseColumns(List<Column> columns) {
        Map<String, Object> record = new HashMap<>();
        for (Column column : columns) {
            record.put(column.getName(), column.getValue());
        }
        return record;
    }
}
