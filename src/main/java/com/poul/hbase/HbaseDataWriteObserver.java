package com.poul.hbase;

import java.io.IOException;
import java.util.Optional;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.poul.hbase.context.ApplicationConfig;
import com.poul.hbase.context.Environment;
import com.poul.hbase.dto.DataModifyMessage;
import com.poul.hbase.mq.KafkaProducerSingleton;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author 杨霄鹏
 * @since 2019/7/16
 */
public class HbaseDataWriteObserver implements RegionObserver, RegionCoprocessor {

    private static final Logger log = LoggerFactory.getLogger(HbaseDataWriteObserver.class);

    private Gson gson = new GsonBuilder().serializeNulls().create();

    private String tableName;
    private String namespace;

    public HbaseDataWriteObserver() {

    }

    @Override
    public Optional<RegionObserver> getRegionObserver() {
        return Optional.of(this);
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        log.info("启动:{}", env.toString());
        Configuration configuration = env.getConfiguration();
        String copEnv = configuration.get("copEnv");
        if (StringUtils.isBlank(copEnv)) {
            copEnv = Environment.DEV.name();
        }

        Environment environment = Environment.valueOf(copEnv.toUpperCase());
        configuration.forEach(conf -> {
            log.info("env.config.{}={}", conf.getKey(), conf.getValue());
        });
        log.info("env.instance{}", env.getInstance());
        log.info("env.classloader{}", env.getClassLoader());
        log.info("env.loadSequence{}", env.getLoadSequence());

        ApplicationConfig.readingProperties(environment);

        KafkaProducerSingleton.getInstance();

        // 获取表名
        String tableNameKey = ApplicationConfig.ApplicationProperties.getProperty("coprocessor.arg.table-name.key");
        if (StringUtils.isBlank(tableNameKey)) {
            tableNameKey = "tableName";
        }
        tableName = configuration.get(tableNameKey);
        // 获取命名空间
        String namespaceKey = ApplicationConfig.ApplicationProperties.getProperty("coprocessor.arg.namespace.key");
        if (StringUtils.isBlank(namespaceKey)) {
            namespaceKey = "namespace";
        }
        namespace = configuration.get(namespaceKey);
        if (StringUtils.isBlank(namespace)) {
            namespace = "default";
        }

    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        log.info("停止:{}", env.toString());
        KafkaProducerSingleton.destroy();
        ApplicationConfig.destroy();
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> c, Put put, WALEdit edit, Durability durability)
            throws IOException {
        log.info("post put {},c : {},edit: {},durability: {}", put.toJSON(), c, edit, durability);
        DataModifyMessage dataModifyMessage = new DataModifyMessage().setNamespace(getNamespace())
                .setTableName(getTableName());
        dataModifyMessage.setAction("put");
        dataModifyMessage.setRowKey(new String(put.getRow()));
        dataModifyMessage.setMeta(put.toJSON());
        KafkaProducerSingleton.getInstance().sendMessage(gson.toJson(dataModifyMessage));
    }

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> c, Delete delete, WALEdit edit,
                           Durability durability) throws IOException {
        log.info("post delete {},c : {},edit: {},durability: {}", delete.toJSON(), c, edit, durability);
        DataModifyMessage dataModifyMessage = new DataModifyMessage().setNamespace(getNamespace())
                .setTableName(getTableName());
        dataModifyMessage.setAction("delete");
        dataModifyMessage.setRowKey(new String(delete.getRow()));
        dataModifyMessage.setMeta(delete.toJSON());
        KafkaProducerSingleton.getInstance().sendMessage(gson.toJson(dataModifyMessage));
    }

    /**
     * 获取表明
     *
     * @return
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * 获取命名空间
     *
     * @return
     */
    public String getNamespace() {
        return StringUtils.isBlank(namespace) ? "default" : namespace;
    }

}
