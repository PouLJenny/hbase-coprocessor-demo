package com.poul.hbase.dto;

import lombok.Data;
import lombok.experimental.Accessors;

import java.io.Serializable;

/**
 * 数据修改消息
 * @author 杨霄鹏
 * @since 2019/7/22
 */
@Data
@Accessors(chain = true)
public class DataModifyMessage implements Serializable {

    /**
     * hbase的rowKey
     */
    private String rowKey;
    /**
     * put delete
     */
    private String action;
    /**
     * 元数据
     */
    private Object meta;

    /**
     * 表名
     */
    private String tableName;
    /**
     * 命名空间
     */
    private String namespace;

}
