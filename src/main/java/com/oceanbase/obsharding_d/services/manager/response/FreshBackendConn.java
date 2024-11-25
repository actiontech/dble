/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbGroup;
import com.oceanbase.obsharding_d.btrace.provider.ConnectionPoolProvider;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

public final class FreshBackendConn {
    private static final Logger LOGGER = LoggerFactory.getLogger(FreshBackendConn.class);

    private FreshBackendConn() {
    }

    public static void execute(ManagerService service, Matcher matcher, boolean isForced) {
        String groupName = matcher.group(1);
        String instanceNames = matcher.group(3);

        final ReentrantReadWriteLock lock = OBsharding_DServer.getInstance().getConfig().getLock();
        if (lock.writeLock().tryLock()) {
            try {
                ConnectionPoolProvider.freshConnGetRealodLocekAfter();
                PhysicalDbGroup dh = OBsharding_DServer.getInstance().getConfig().getDbGroups().get(groupName);
                if (dh == null) {
                    service.writeErrMessage(ErrorCode.ER_YES, "dbGroup " + groupName + " do not exists");
                    return;
                }

                if (!dh.checkInstanceExist(instanceNames)) {
                    service.writeErrMessage(ErrorCode.ER_YES, "Some of the dbInstance in command in " + dh.getGroupName() + " do not exists");
                    return;
                }

                String warnMsg = null;
                try {
                    String[] nameList = instanceNames == null ? Arrays.copyOf(dh.getAllDbInstanceMap().keySet().toArray(), dh.getAllDbInstanceMap().keySet().toArray().length, String[].class) : instanceNames.split(",");
                    List<String> sourceNames = Arrays.stream(nameList).distinct().collect(Collectors.toList());

                    if (dh.getRwSplitMode() == PhysicalDbGroup.RW_SPLIT_OFF && (!sourceNames.contains(dh.getWriteDbInstance().getName()))) {
                        warnMsg = "the rwSplitMode of this dbGroup is 0, so connection pool for slave dbInstance don't refresh";
                    } else {
                        if (dh.getRwSplitMode() == PhysicalDbGroup.RW_SPLIT_OFF && sourceNames.size() > 1 && sourceNames.contains(dh.getWriteDbInstance().getName())) {
                            warnMsg = "the rwSplitMode of this dbGroup is 0, so connection pool for slave dbInstance don't refresh";
                        }
                        dh.stopOfFresh(sourceNames, "fresh backend conn", isForced);
                        dh.startOfFresh(sourceNames, "fresh backend conn");
                    }
                } catch (Exception e) {
                    service.writeErrMessage(ErrorCode.ER_YES, "fresh conn with error, use show @@backend to check latest status. Error:" + e.getMessage());
                    LOGGER.warn("fresh conn with error", e);
                    return;
                }

                OkPacket packet = new OkPacket();
                packet.setPacketId(1);
                packet.setAffectedRows(0);
                if (warnMsg != null) {
                    packet.setMessage(warnMsg.getBytes());
                }
                packet.setServerStatus(2);
                packet.write(service.getConnection());
            } finally {
                lock.writeLock().unlock();
            }
        } else {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "may be other mutex events that cause interrupt, try again later");
        }
    }
}
