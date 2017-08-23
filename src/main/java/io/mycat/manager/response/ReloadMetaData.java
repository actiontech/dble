package io.mycat.manager.response;

import io.mycat.MycatServer;
import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.OkPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReloadMetaData {
    private static final Logger LOGGER = LoggerFactory.getLogger(ReloadSqlSlowTime.class);

    public static void execute(ManagerConnection c) {
        String msg = "datahost has no write_host";
        if (!MycatServer.getInstance().getConfig().isDataHostWithoutWR()) {
            MycatServer.getInstance().reloadMetaData(MycatServer.getInstance().getConfig());
            msg = "reload metadata sucess";
        }
        LOGGER.info(msg);
        OkPacket ok = new OkPacket();
        ok.packetId = 1;
        ok.affectedRows = 1;
        ok.serverStatus = 2;
        ok.message = msg.getBytes();
        ok.write(c);
    }
}
