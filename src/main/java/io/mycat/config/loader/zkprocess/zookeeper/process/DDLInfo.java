package io.mycat.config.loader.zkprocess.zookeeper.process;

/**
 * Created by huqing.yan on 2017/6/7.
 */
public class DDLInfo {
    public enum DDLStatus {
        INIT, SUCCESS, FAILED
    }

    private String schema;
    private String sql;
    private String from;
    private DDLStatus status;
    private String split = ";";

    public DDLInfo(String schema, String sql, String from, DDLStatus ddlStatus) {
        this.schema = schema;
        this.sql = sql;
        this.from = from;
        this.status = ddlStatus;
    }

    public DDLInfo(String info) {
        String[] infos = info.split(split);
        this.schema = infos[0];
        this.sql = infos[1];
        this.from = infos[2];
        this.status = DDLStatus.valueOf(infos[3]);
    }

    @Override
    public String toString() {
        return schema + split + sql + split + from + split + status.toString();
    }

    public String getFrom() {
        return from;
    }

    public String getSchema() {
        return schema;
    }

    public String getSql() {
        return sql;
    }

    public DDLStatus getStatus() {
        return status;
    }

}
