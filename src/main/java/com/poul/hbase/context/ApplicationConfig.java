package com.poul.hbase.context;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author 杨霄鹏
 * @since 2019/7/23
 */
public class ApplicationConfig {

    private static final Logger logger = LoggerFactory.getLogger(ApplicationConfig.class);

    public static Properties ApplicationProperties;

    public static Properties KafkaProperties;

    public static Environment currentEnvironment;

    public static void readingProperties(Environment environment) {
        if (environment == null) {
            environment = Environment.defaultEnv();
        }

        currentEnvironment = environment;
        logger.info("开始加载配置，当前环境：{}",currentEnvironment.name().toLowerCase());

        InputStream resourceAsStream = ApplicationConfig.class.getResourceAsStream("/kafka-" + currentEnvironment.name().toLowerCase() + ".properties");
        Properties properties = new Properties();
        try {
            properties.load(resourceAsStream);
            KafkaProperties = properties;
            resourceAsStream.close();
        } catch (IOException e) {
            logger.error("读取kafka的配置文件错误，当前环境： {}",currentEnvironment.name().toLowerCase(),e);
        }

        logger.info("kafka配置加载完毕：{}",KafkaProperties.toString());

        resourceAsStream = ApplicationConfig.class.getResourceAsStream("/application-" + currentEnvironment.name().toLowerCase() + ".properties");
        Properties appProperties = new Properties();
        try {
            appProperties.load(resourceAsStream);
            ApplicationProperties = appProperties;
            resourceAsStream.close();
        } catch (Exception e) {
            logger.error("读取应用的配置文件错误，当前环境： {}",currentEnvironment.name().toLowerCase(),e);
        }

        logger.info("应用配置加载完毕：{}",ApplicationProperties.toString());
    }


    public static void destroy() {
        KafkaProperties = null;
        ApplicationProperties = null;
        currentEnvironment = null;
    }

}
