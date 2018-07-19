/*
 * Copyright (C) 2016-2018 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.trace;

public class TraceRecord {
    private final String dataNode;
    private final String ref;
    private long timestamp;

    public TraceRecord(long timestamp) {
        this(timestamp, "-", "-");
    }

    public TraceRecord(long timestamp, String dataNode, String ref) {
        this.timestamp = timestamp;
        this.dataNode = dataNode;
        this.ref = ref;
    }
    public String getDataNode() {
        return dataNode;
    }

    public String getRef() {
        return ref;
    }

    public long getTimestamp() {
        return timestamp;
    }

}
