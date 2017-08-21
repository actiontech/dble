package io.mycat.config.loader.console;

/**
 * 专门用来操作zookeeper路径的文件信息
 * 源文件名：ZkPath.java
 * 文件版本：1.0.0
 * 创建作者：liujun
 * 创建日期：2016年9月15日
 * 修改作者：liujun
 * 修改日期：2016年9月15日
 * 文件描述：TODO
 * 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
 */
public enum ZookeeperPath {

    /**
     * zk写入本地的路径信息
     */
    ZK_LOCAL_WRITE_PATH("./"),;
    /**
     * 配制的key的信息
     */
    private String key;

    ZookeeperPath(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }


}
