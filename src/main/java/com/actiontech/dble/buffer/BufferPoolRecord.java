/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.buffer;

/**
 * @author dcy
 * Create Date: 2022-10-14
 */
public class BufferPoolRecord {

    private String[] stacktrace;
    private String sql;
    private BufferType type;
    private int allocateLength;
    private long allocatedTime;

    public BufferPoolRecord(String[] stacktrace, String sql, BufferType type, int allocateLength, long allocatedTime) {
        this.stacktrace = stacktrace;
        this.sql = sql;
        this.type = type;
        this.allocateLength = allocateLength;
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

    public int getAllocateLength() {
        return allocateLength;
    }

    public long getAllocatedTime() {
        return allocatedTime;
    }

    public static final class Builder {
        private String[] stacktrace;
        private String sql;
        private BufferType type = BufferType.NORMAL;
        private int allocateLength;
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

        public Builder withAllocateLength(int allocateLengthTmp) {
            this.allocateLength = allocateLengthTmp;
            return this;
        }

        public Builder withAllocatedTime(long allocatedTimeTmp) {
            this.allocatedTime = allocatedTimeTmp;
            return this;
        }

        public BufferPoolRecord build() {
            return new BufferPoolRecord(stacktrace, sql, type, allocateLength, allocatedTime);
        }
    }
}
