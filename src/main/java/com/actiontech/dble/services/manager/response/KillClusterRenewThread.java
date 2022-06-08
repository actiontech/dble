package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.cluster.ClusterGeneralConfig;
import com.actiontech.dble.cluster.general.AbstractConsulSender;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.util.StringUtil;

public final class KillClusterRenewThread {
    private KillClusterRenewThread() {
    }

    public static void response(ManagerService service, String name) {
        String str = getResult(name);

        OkPacket packet = new OkPacket();
        packet.setPacketId(1);
        packet.setAffectedRows(0);
        packet.setMessage(str.getBytes());
        packet.setServerStatus(2);
        packet.write(service.getConnection());
    }

    public static String getResult(String name) {
        if (ClusterConfig.getInstance().isClusterEnable() &&
                ClusterGeneralConfig.getInstance().getClusterSender() instanceof AbstractConsulSender) {
            AbstractConsulSender clusterSender = (AbstractConsulSender) ClusterGeneralConfig.getInstance().getClusterSender();
            name = StringUtil.removeAllApostrophe(name);
            if (name.startsWith(clusterSender.getRenewThreadPrefix())) {
                String path = name.substring(clusterSender.getRenewThreadPrefix().length());
                boolean isKill = clusterSender.killRenewThread(path);
                if (isKill) {
                    return "kill cluster renew thread successfully!";
                }
            }
        }
        return "There is no cluster renew thread!";
    }
}
