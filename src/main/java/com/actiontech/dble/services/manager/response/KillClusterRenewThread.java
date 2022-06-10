package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.cluster.ClusterGeneralConfig;
import com.actiontech.dble.cluster.general.AbstractConsulSender;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.util.StringUtil;

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
                boolean isKill = clusterSender.killRenewThread(path);
                if (isKill) {
                    return new Object[]{true, "kill cluster renew thread successfully!"};
                }
            }
        }
        return new Object[]{false, "wrong cluster renew thread!"};
    }
}
