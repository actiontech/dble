/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.trace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraceRecord implements Cloneable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TraceRecord.class);
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

    @Override
    public Object clone() {
        TraceRecord obj;
        try {
            obj = (TraceRecord) super.clone();
            return obj;
        } catch (Exception e) {
            LOGGER.warn("clone TraceRecord error", e);
            throw new AssertionError(e.getMessage());
        }
    }
}
