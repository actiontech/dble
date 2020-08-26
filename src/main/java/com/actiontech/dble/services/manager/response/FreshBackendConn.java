package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.services.manager.ManagerService;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

public final class FreshBackendConn {

    private FreshBackendConn() {
    }

    public static void execute(ManagerService service, Matcher matcher, boolean isForced) {
        String groupName = matcher.group(1);
        String instanceNames = matcher.group(3);

        //check the dbGroup is exists
        final ReentrantReadWriteLock lock = DbleServer.getInstance().getConfig().getLock();
        lock.readLock().lock();
        try {
            PhysicalDbGroup dh = DbleServer.getInstance().getConfig().getDbGroups().get(groupName);
            if (dh == null) {
                service.writeErrMessage(ErrorCode.ER_YES, "dbGroup " + groupName + " do not exists");
                return;
            }

            if (!dh.checkInstanceExist(instanceNames)) {
                service.writeErrMessage(ErrorCode.ER_YES, "Some of the dbInstance in command in " + dh.getGroupName() + " do not exists");
                return;
            }

            String warnMsg = null;
            // single
            try {
                String[] nameList = instanceNames == null ? Arrays.copyOf(dh.getAllDbInstanceMap().keySet().toArray(), dh.getAllDbInstanceMap().keySet().toArray().length, String[].class) : instanceNames.split(",");
                List<String> sourceNames = Arrays.asList(nameList).stream().distinct().collect(Collectors.toList());

                if (dh.getRwSplitMode() == PhysicalDbGroup.RW_SPLIT_OFF && (!sourceNames.contains(dh.getWriteDbInstance().getName()))) {
                    warnMsg = "the rwSplitMode of this dbGroup is 0, so connection pool for slave dbInstance don't refresh";
                } else {
                    if (dh.getRwSplitMode() == PhysicalDbGroup.RW_SPLIT_OFF && sourceNames.size() > 1 && sourceNames.contains(dh.getWriteDbInstance().getName())) {
                        warnMsg = "the rwSplitMode of this dbGroup is 0, so connection pool for slave dbInstance don't refresh";
                    }
                    dh.stop(sourceNames, "fresh backend conn", isForced);
                    dh.init(sourceNames, "fresh backend conn");
                }
            } catch (Exception e) {
                service.writeErrMessage(ErrorCode.ER_YES, "disable dbGroup with error, use show @@backend to check latest status. Error:" + e.getMessage());
                e.printStackTrace();
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
            lock.readLock().unlock();
        }
    }
}
