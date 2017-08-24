package io.mycat.manager.response;

import io.mycat.manager.ManagerConnection;
import io.mycat.net.mysql.OkPacket;
import io.mycat.statistic.stat.UserStat;
import io.mycat.statistic.stat.UserStatAnalyzer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public final class ReloadSqlSlowTime {
    private ReloadSqlSlowTime() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ReloadSqlSlowTime.class);

    public static void execute(ManagerConnection c, long time) {

        Map<String, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
        for (UserStat userStat : statMap.values()) {
            userStat.setSlowTime(time);
        }

        LOGGER.warn(String.valueOf(c) + "Reset show  @@sql.slow=" + time + " time success by manager");

        OkPacket ok = new OkPacket();
        ok.packetId = 1;
        ok.affectedRows = 1;
        ok.serverStatus = 2;
        ok.message = "Reset show  @@sql.slow time success".getBytes();
        ok.write(c);
    }

}
