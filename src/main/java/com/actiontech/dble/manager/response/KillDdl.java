/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.btrace.provider.ClusterDelayProvider;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.loader.ucoreprocess.ClusterUcoreSender;
import com.actiontech.dble.config.loader.ucoreprocess.UDistrbtLockManager;
import com.actiontech.dble.config.loader.ucoreprocess.UcorePathUtil;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.util.KVPathUtil;
import com.actiontech.dble.util.StringUtil;
import com.actiontech.dble.util.ZKUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;

import static com.actiontech.dble.route.parser.ManagerParseShow.PATTERN_FOR_TABLE_INFO;

/**
 * @author mycat
 */
public final class KillDdl {
    private KillDdl() {
    }
    private static final Logger LOGGER = LoggerFactory.getLogger(KillDdl.class);

    public static void response(String stmt, String tableInfo, ManagerConnection mc) {
        LOGGER.info("execute kill ddl sql:'" + stmt + "'");
        Matcher matcher = PATTERN_FOR_TABLE_INFO.matcher(tableInfo);
        if (!matcher.matches()) {
            mc.writeErrMessage(ErrorCode.ER_YES, "Unsupported statement");
            return;
        }
        String schema = matcher.group(2);
        String table = matcher.group(4);

        int count = DbleServer.getInstance().getTmManager().removeMetaLock(schema, table);
        // remove distributed lock
        try {
            String nodeName = StringUtil.getUFullName(schema, table);
            if (DbleServer.getInstance().isUseZK()) {
                CuratorFramework zkConn = ZKUtils.getConnection();
                String nodePath = ZKPaths.makePath(KVPathUtil.getDDLPath(), nodeName);
                zkConn.delete().deletingChildrenIfNeeded().forPath(nodePath);
            } else if (DbleServer.getInstance().isUseUcore()) {
                ClusterDelayProvider.delayBeforeDdlNoticeDeleted();
                ClusterUcoreSender.deleteKVTree(UcorePathUtil.getDDLPath(nodeName) + "/");
                ClusterDelayProvider.delayBeforeDdlLockRelease();
                UDistrbtLockManager.releaseLock(UcorePathUtil.getDDLPath(nodeName));
            }
        } catch (Exception e) {
            mc.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "kill ddl '" + stmt + "' distribute lock error, because " + e.getMessage());
            return;
        }

        OkPacket packet = new OkPacket();
        packet.setPacketId(1);
        packet.setAffectedRows(count);
        packet.setServerStatus(2);
        packet.write(mc);
    }

}
