/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.cluster.ClusterGeneralConfig;
import com.oceanbase.obsharding_d.cluster.general.AbstractConsulSender;
import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.model.ClusterConfig;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.util.StringUtil;

public final class KillClusterRenewThread {
    private KillClusterRenewThread() {
    }

    public static void response(ManagerService service, String name) {
        Object[] result = getResult(name);

        boolean isSuccess = (boolean) result[0];
        String message = (String) result[1];
        if (isSuccess) {
            OkPacket packet = new OkPacket();
            packet.setPacketId(1);
            packet.setAffectedRows(0);
            packet.setMessage(message.getBytes());
            packet.setServerStatus(2);
            packet.write(service.getConnection());
        } else {
            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, message);
        }
    }

    public static Object[] getResult(String name) {
        if (ClusterConfig.getInstance().isClusterEnable() &&
                ClusterGeneralConfig.getInstance().getClusterSender() instanceof AbstractConsulSender) {
            AbstractConsulSender clusterSender = (AbstractConsulSender) ClusterGeneralConfig.getInstance().getClusterSender();
            name = StringUtil.removeAllApostrophe(name);
            if (name.startsWith(clusterSender.getRenewThreadPrefix())) {
                String path = name.substring(clusterSender.getRenewThreadPrefix().length());

                if (path.endsWith(ClusterPathUtil.getOnlinePath(SystemConfig.getInstance().getInstanceName())))
                    return new Object[]{false, "the cluster 'online' renew thread is not allowed to be killed!"};

                boolean isKill = clusterSender.killRenewThread(path);
                if (isKill) {
                    return new Object[]{true, "kill cluster renew thread successfully!"};
                }
            }
        }
        return new Object[]{false, "wrong cluster renew thread!"};
    }
}
