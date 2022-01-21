/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.xa;

import java.io.Serializable;

/**
 * Created by zhangchao on 2016/10/17.
 */
public class ParticipantLogEntry implements Serializable {

    private static final long serialVersionUID = 1728296701394899871L;

    /**
     * The ID of the global transaction as known by the transaction core.
     */

    private String coordinatorId;
    /**
     * Identifies the participant within the global transaction.
     */

    private String host;

    private int port;
    /**
     * When does this participant expire (expressed in millis since Jan 1,
     * 1970)?
     */
    private long expires;

    /**
     * Best-known state of the participant.
     */
    private TxState txState;
    /**
     * For diagnostic purposes, null if not relevant.
     */
    private String schema;

    private String tableName;

    private long repeatTableIndex;

    public ParticipantLogEntry(String coordinatorId, String host, int port, long expires, String schema,
                               TxState txState, String tableName, long repeatTableIndex) {
        this.coordinatorId = coordinatorId;
        this.host = host;
        this.port = port;
        this.expires = expires;
        this.schema = schema;
        this.txState = txState;
        this.tableName = tableName;
        this.repeatTableIndex = repeatTableIndex;
    }

    public boolean compareAddress(String hostName, int portNum, String schemaName) {
        return this.host.equals(hostName) && this.port == portNum && this.schema.equals(schemaName);
    }

    @Override
    public String toString() {
        return "ParticipantLogEntry [id=" + coordinatorId + ", host=" + host + ", port=" + port + ", expires=" + expires +
                ", state=" + txState + ", schema=" + schema + ", table=" + tableName + ", repeatTableIndex=" + repeatTableIndex + "]";
    }

    public String getCoordinatorId() {
        return coordinatorId;
    }

    public void setCoordinatorId(String coordinatorId) {
        this.coordinatorId = coordinatorId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public long getExpires() {
        return expires;
    }

    public void setExpires(long expires) {
        this.expires = expires;
    }

    public TxState getTxState() {
        return txState;
    }

    public void setTxState(TxState txState) {
        this.txState = txState;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public long getRepeatTableIndex() {
        return repeatTableIndex;
    }

    public void setRepeatTableIndex(long repeatTableIndex) {
        this.repeatTableIndex = repeatTableIndex;
    }
}
