/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.buffer;

/**
 * @author dcy
 * Create Date: 2022-10-14
 */
public class BufferPoolRecord {

    private String[] stacktrace;
    private String sql;
    private BufferType type;
    private int allocateSize;
    private long allocatedTime;

    public BufferPoolRecord(String[] stacktrace, String sql, BufferType type, int allocateSize, long allocatedTime) {
        this.stacktrace = stacktrace;
        if (sql != null) {
            sql = sql.length() > 1024 ? sql.substring(0, 1024) + "..." : sql;
        }
        this.sql = sql;
        this.type = type;
        this.allocateSize = allocateSize;
        this.allocatedTime = allocatedTime;
    }

    public static Builder builder() {
        return new Builder();
    }


    public String[] getStacktrace() {
        return stacktrace;
    }

    public String getSql() {
        return sql;
    }

    public BufferType getType() {
        return type;
    }

    public int getAllocateSize() {
        return allocateSize;
    }

    public long getAllocatedTime() {
        return allocatedTime;
    }

    @Override
    public String toString() {
        return "BufferPoolRecord{" +
                ", sql='" + sql + '\'' +
                ", type=" + type +
                ", allocateSize=" + allocateSize +
                ", allocatedTime=" + allocatedTime +
                '}';
    }

    public static final class Builder {
        private String[] stacktrace;
        private String sql;
        private BufferType type = BufferType.NORMAL;
        private int allocateSize;
        private long allocatedTime;

        private Builder() {
        }

        public BufferType getType() {
            return type;
        }

        public Builder withStacktrace(String[] stacktraceTmp) {
            this.stacktrace = stacktraceTmp;
            return this;
        }

        public Builder withSql(String sqlTmp) {
            this.sql = sqlTmp;
            return this;
        }

        public Builder withType(BufferType typeTmp) {
            this.type = typeTmp;
            return this;
        }

        public Builder withAllocateSize(int allocateLengthTmp) {
            this.allocateSize = allocateLengthTmp;
            return this;
        }

        public Builder withAllocatedTime(long allocatedTimeTmp) {
            this.allocatedTime = allocatedTimeTmp;
            return this;
        }

        public BufferPoolRecord build() {
            return new BufferPoolRecord(stacktrace, sql, type, allocateSize, allocatedTime);
        }
    }
}
