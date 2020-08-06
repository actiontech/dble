package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.services.manager.ManagerService;

import java.util.Arrays;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;

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

            // single
            try {
                String[] nameList = instanceNames == null ? Arrays.copyOf(dh.getAllDbInstanceMap().keySet().toArray(), dh.getAllDbInstanceMap().keySet().toArray().length, String[].class) : instanceNames.split(",");
                dh.stop(nameList, "fresh backend conn", isForced);
                dh.init(nameList, "fresh backend conn", true);
            } catch (Exception e) {
                service.writeErrMessage(ErrorCode.ER_YES, "disable dbGroup with error, use show @@backend to check latest status. Error:" + e.getMessage());
                e.printStackTrace();
                return;
            }

            OkPacket packet = new OkPacket();
            packet.setPacketId(1);
            packet.setAffectedRows(0);
            packet.setServerStatus(2);
            packet.write(service.getConnection());
        } finally {
            lock.readLock().unlock();
        }
    }
}
