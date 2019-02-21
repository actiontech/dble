/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager.response;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.ManagerConnection;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.util.CollectionUtil;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ReloadMetaData {
    private ReloadMetaData() {
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ReloadMetaData.class);

    public static final Pattern PATTERN_IN = Pattern.compile("^\\s*table\\s+in\\s*\\((('[a-zA-Z_0-9]+\\.[a-zA-Z_0-9]+',)*('[a-zA-Z_0-9]+\\.[a-zA-Z_0-9]+'))\\)\\s*$", Pattern.CASE_INSENSITIVE);
    public static final Pattern PATTERN_WHERE = Pattern.compile("^\\s*schema\\s*=\\s*'([a-zA-Z_0-9]+)'" +
            "(\\s+and\\s+table\\s*=\\s*'([a-zA-Z_0-9]+)')?\\s*$", Pattern.CASE_INSENSITIVE);

    public static void execute(ManagerConnection c, String whereCondition) {
        Map<String, Set<String>> filter = null;
        if (!StringUtil.isEmpty(whereCondition)) {
            filter = getFilter(whereCondition);
            if (CollectionUtil.isEmpty(filter)) {
                c.writeErrMessage(ErrorCode.ER_YES, "Usage: reload @@metadata [where schema='?' [and table='?'] | where table in ('schema1'.'table1',...)]");
                return;
            }
        }

        String msg = "data host has no write_host";
        boolean isOK = true;
        final ReentrantLock lock = DbleServer.getInstance().getTmManager().getMetaLock();
        lock.lock();
        try {
            String checkResult = DbleServer.getInstance().getTmManager().metaCountCheck();
            if (checkResult != null) {
                LOGGER.warn(checkResult);
                c.writeErrMessage("HY000", checkResult, ErrorCode.ER_DOING_DDL);
                return;
            }
            try {
                if (!DbleServer.getInstance().getConfig().isDataHostWithoutWR()) {
                    DbleServer.getInstance().reloadMetaData(DbleServer.getInstance().getConfig(), filter);
                    msg = "reload metadata success";
                }
            } catch (Exception e) {
                isOK = false;
                msg = "reload metadata failed," + e.toString();
            }
        } finally {
            lock.unlock();
        }
        if (isOK) {
            LOGGER.info(msg);
            OkPacket ok = new OkPacket();
            ok.setPacketId(1);
            ok.setAffectedRows(1);
            ok.setServerStatus(2);
            ok.setMessage(msg.getBytes());
            ok.write(c);
        } else {
            LOGGER.warn(msg);
            c.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, msg);
        }
    }

    /**
     * get schemas and tables in where condition
     * @param whereCondition
     * @return
     */
    private static Map<String, Set<String>> getFilter(String whereCondition) {
        Map<String, Set<String>> filter = new HashMap<>();
        Matcher matcher = PATTERN_WHERE.matcher(whereCondition);
        if (matcher.matches()) {
            Set<String> tables = null;
            if (!StringUtil.isEmpty(matcher.group(3))) {
                tables = new HashSet<>(1);
                tables.add(matcher.group(3));
            }
            filter.put(matcher.group(1), tables);
        }
        matcher = PATTERN_IN.matcher(whereCondition);
        if (matcher.matches()) {
            String schemaTable = matcher.group(1);
            for (String s : schemaTable.split(",")) {
                String temp = s.replaceAll("'", "");
                Set<String> tables = filter.get(temp.split("\\.")[0]);
                if (tables == null) {
                    tables = new HashSet<>();
                    filter.put(temp.split("\\.")[0], tables);
                }
                tables.add(temp.split("\\.")[1]);
            }
        }
        return filter;
    }
}
