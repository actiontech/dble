package io.mycat.log.transaction;

public class TxnBinaryLog {
    private String user;
    private String host;
    private String schema;
    private long xid;
    private String executeTime;
    private String query;
    private long connid;

    public long getConnid() {
        return connid;
    }

    public void setConnid(long connid) {
        this.connid = connid;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public long getXid() {
        return xid;
    }

    public void setXid(long xid) {
        this.xid = xid;
    }

    public String getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(String executeTime) {
        this.executeTime = executeTime;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(executeTime).append(", ConnID:").append(connid).append(", XID:").append(xid).append(", ").
                append("MySQL user ").append("'").append(user).
                append("'").append("@'").append(host).append("', ").append(" Current schema `").append(schema).
                append("`, ").append("Current query ").append("\n").append(query).append("\n");
        return builder.toString();
    }
}
