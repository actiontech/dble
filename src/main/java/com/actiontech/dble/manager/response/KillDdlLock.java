/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.cluster.DistrbtLockManager;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;

import static com.actiontech.dble.route.parser.ManagerParseShow.PATTERN_FOR_TABLE_INFO;

/**
 * @author mycat
 */
public final class KillDdlLock {
    private KillDdlLock() {
    }
    private static final Logger LOGGER = LoggerFactory.getLogger(KillDdlLock.class);

    public static void response(String stmt, String tableInfo, ManagerConnection mc) {
        LOGGER.info("execute kill ddl lock:'" + stmt + "'");
        Matcher matcher = PATTERN_FOR_TABLE_INFO.matcher(tableInfo);
        if (!matcher.matches()) {
            mc.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
            return;
        }
        String schema = matcher.group(2);
        String table = matcher.group(4);
        // release distributed lock
        if (DbleServer.getInstance().isUseGeneralCluster()) {
            DistrbtLockManager.releaseLock(ClusterPathUtil.getDDLPath(StringUtil.getUFullName(schema, table)));
        }
        boolean isRemoved = DbleServer.getInstance().getTmManager().removeMetaLock(schema, table);
        OkPacket packet = new OkPacket();
        packet.setPacketId(1);
        packet.setAffectedRows(0);
        if (isRemoved) {
            packet.setMessage(("ddl lock is removed successfully!").getBytes());
        } else {
            packet.setMessage(("There is no ddl lock!").getBytes());
        }
        packet.setServerStatus(2);
        packet.write(mc);
    }

}
