package com.iflytek.flume.dics.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;


/**
 * @author cyh
 * @Date 9:00 2019/7/17
 * @description
 * @since 2.0
 */
public class KafkaSink extends AbstractSink implements Configurable {

    private KafkaProducer<String, String> producer;
    private org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaSink.class);
    private static final String TOPIC = "kafka.topic";
    private static final String SERVERS = "kafka.bootstrap.servers";
    private String topic;
    private String servers;
    @Override
    public Status process() throws EventDeliveryException {
        Status status = null;
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();
        txn.begin();
        try {

            Event event = ch.take();
            if(event == null) {
                status = Status.BACKOFF;
            }
            byte[] byte_message = event.getBody();
            Map<String,String> heads =event.getHeaders();

            logger.info("传送信息到kafka》》》》》》》》》》》》 "+new String(byte_message));
            //生产者
            ProducerRecord<String, String> record =new ProducerRecord<>(topic, new String(byte_message));
            producer.send(record);
            txn.commit();
            status = Status.READY;
        } catch (Throwable t) {
            txn.rollback();
            status = Status.BACKOFF;
            if (t instanceof Error) {
                throw (Error)t;
            }
        }finally {
            txn.close();
        }
        return status;
    }


    @Override
    public void configure(Context context) {
        topic = context.getString(TOPIC);
        servers = context.getString(SERVERS);
        Properties originalProps = new Properties();
        originalProps.put("bootstrap.servers", servers);
        originalProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        originalProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        originalProps.put("custom.encoding", "UTF-8");
        producer = new KafkaProducer<String,String>(originalProps);

    }
}
