package io.mycat.config.loader.console;

import io.mycat.config.Versions;

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
     * zk的路径分隔符
    */
    ZK_SEPARATOR("/"),

    /**
     * 最基础的mycat节点
     */
    FLOW_ZK_PATH_BASE(Versions.ROOT_PREFIX),

    /**
     * 在当前在线的节点
    */
    FLOW_ZK_PATH_ONLINE("online"),

    /**
     * schema父路径
    */
    FOW_ZK_PATH_SCHEMA("schema"),

    /**
     * 配制schema信息
     */
    FLOW_ZK_PATH_SCHEMA_SCHEMA("schema"),

    /**
     * 对应数据库信息
    */
    FLOW_ZK_PATH_SCHEMA_DATANODE("dataNode"),

    /**
     * 数据库信息dataHost
     */
    FLOW_ZK_PATH_SCHEMA_DATAHOST("dataHost"),

    /**
     * 路由信息
     */
    FLOW_ZK_PATH_RULE("rules"),

    /**
     * 路由信息
     */
    FLOW_ZK_PATH_RULE_TABLERULE("tableRule"),

    /**
     * 路由信息
     */
    FLOW_ZK_PATH_RULE_FUNCTION("function"),

    /**
     * 服务端配制路径
    */
    FLOW_ZK_PATH_SERVER("server"),

    /**
     * 默认配制信息
    */
    FLOW_ZK_PATH_SERVER_DEFAULT("default"),


    /**
     * 配制的用户信息
     */
    //TODO:user.privileges IS READY?
    FLOW_ZK_PATH_SERVER_USER("user"),

    /**
     * 配制的防火墙信息,如黑白名单信息
     */
    //TODO:Firewall
    FLOW_ZK_PATH_SERVER_FIREWALL("firewall"),


    /**
     * 序列信息
     */
    FLOW_ZK_PATH_SEQUENCE("sequences"),

    /**
     * 序列信息中公共配制信息
     */
    FLOW_ZK_PATH_SEQUENCE_COMMON("common"),

   ZK_PATH_INSTANCE("instance"),

    /**
     * 用来存放序列值的
     */
    FLOW_ZK_PATH_SEQUENCE_LEADER("leader"),
    
    /**
     * 递增序列号
     */
    FLOW_ZK_PATH_SEQUENCE_INCREMENT_SEQ("incr_sequence"),

    /**
     * 缓存信息
    */
    FLOW_ZK_PATH_CACHE("cache"),

    /**
     * 配制切换及状态目录信息
    */
    FLOW_ZK_PATH_BINDATA("bindata"),

    /**zk写入本地的路径信息
    */
    ZK_LOCAL_WRITE_PATH("./"),

    /**
     * zk本地配制目录信息
     */
    ZK_LOCAL_CFG_PATH("/zkconf/"),
    /**
     * conf 是否初始化过的标记
     */
    ZK_CONF_INITED("confInitialized"),
    //use for sync ddl meta data
    ZK_DDL("ddl"),
    ZK_LOCK("lock"),
    ;
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

    public void setKey(String key) {
        this.key = key;
    }

}
