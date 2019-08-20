package com.poul.hbase.mq;

import com.poul.hbase.context.ApplicationConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author 杨霄鹏
 * @since 2019/7/22
 */
public class KafkaProducerSingleton {

    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerSingleton.class);

    private Producer<String, String> producer;

    private String defaultTopic;

    private static KafkaProducerSingleton instance;

    void setProducer(Producer producer) {
        this.producer = producer;
    }

    void setDefaultTopic(String defaultTopic) {
        this.defaultTopic = defaultTopic;
    }


    public static KafkaProducerSingleton getInstance() {
        if (instance == null) {
            synchronized (KafkaProducerSingleton.class) {
                if (instance == null) {
                    logger.info("初始化kafka连接信息");
                    // 初始化
                    init();
                }
            }
        }
        return instance;
    }


    private static void init() {
        instance = new KafkaProducerSingleton();
        instance.setProducer(new KafkaProducer<String, String>(ApplicationConfig.KafkaProperties));
        // 读取应用配置
        instance.setDefaultTopic(ApplicationConfig.ApplicationProperties.getProperty("kafka.topic"));
    }

    /**
     * 发送消息
     *
     * @param message
     */
    public void sendMessage(String message) {
        if (StringUtils.isBlank(message)) {
            return;
        }
        producer.send(new ProducerRecord<>(defaultTopic, message));
        producer.flush();
    }

    public static void destroy() {
        if (instance != null && instance.producer != null) {
            instance.producer.close();
        }
        instance = null;
    }

}
