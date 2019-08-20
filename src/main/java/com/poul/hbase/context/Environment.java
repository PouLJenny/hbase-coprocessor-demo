package com.poul.hbase.context;

/**
 * 环境
 * @author 杨霄鹏
 * @since 2019/7/22
 */
public enum  Environment {

    DEV,TEST,PRE,PROD;


    public static Environment defaultEnv() {
        return DEV;
    }

}
