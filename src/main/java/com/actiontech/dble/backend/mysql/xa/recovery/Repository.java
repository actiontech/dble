package com.actiontech.dble.backend.mysql.xa.recovery;

import com.actiontech.dble.backend.mysql.xa.CoordinatorLogEntry;

import java.util.Collection;

/**
 * Created by zhangchao on 2016/10/13.
 */
public interface Repository {

    void init();

    void put(String id, CoordinatorLogEntry coordinatorLogEntry);

    void remove(String id);

    CoordinatorLogEntry get(String coordinatorId);

    Collection<CoordinatorLogEntry> getAllCoordinatorLogEntries();

    boolean writeCheckpoint(Collection<CoordinatorLogEntry> checkpointContent);

    void close();

}
