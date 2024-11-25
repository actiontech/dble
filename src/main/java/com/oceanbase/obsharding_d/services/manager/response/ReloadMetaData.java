/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager.response;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.cluster.values.ConfStatus;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.meta.ReloadLogHelper;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.meta.ReloadManager;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.singleton.ProxyMeta;
import com.oceanbase.obsharding_d.util.CollectionUtil;
import com.oceanbase.obsharding_d.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.oceanbase.obsharding_d.meta.ReloadStatus.TRIGGER_TYPE_COMMAND;

public final class ReloadMetaData {
    public static final Pattern PATTERN_IN = Pattern.compile("^\\s*table\\s+in\\s*\\(" +
            "(('((?!')((?!\\.)((?!`).)))+\\.((?!')((?!\\.)((?!`).)))+',)*" +
            "'((?!')((?!\\.)((?!`).)))+\\.((?!')((?!\\.)((?!`).)))+')" +
            "\\)\\s*$", Pattern.CASE_INSENSITIVE);
    public static final Pattern PATTERN_WHERE = Pattern.compile("^\\s*schema\\s*=\\s*" +
            "(('|\")((?!`)((?!\\2).))+\\2|[a-zA-Z_0-9\\-]+)" +
            "(\\s+and\\s+table\\s*=\\s*" +
            "(('|\")((?!`)((?!\\7).))+\\7|[a-zA-Z_0-9\\-]+)" +
            ")?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Logger LOGGER = LoggerFactory.getLogger(ReloadMetaData.class);

    private ReloadMetaData() {
    }

    public static void execute(ManagerService service, String whereCondition) {
        Map<String, Set<String>> filter = null;
        if (!StringUtil.isEmpty(whereCondition)) {
            filter = getFilter(whereCondition);
            if (CollectionUtil.isEmpty(filter)) {
                service.writeErrMessage(ErrorCode.ER_YES, "Usage: reload @@metadata [where schema='?' [and table='?'] | where table in ('schema1'.'table1',...)]");
                return;
            }
        }

        String msg = "dbGroup has no primary dbInstance";
        boolean isOK = true;
        boolean interrupt = false;
        final ReentrantLock lock = ProxyMeta.getInstance().getTmManager().getMetaLock();
        lock.lock();
        try {
            ReloadLogHelper.graceInfo("added metaLock");
            String checkResult = ProxyMeta.getInstance().getTmManager().metaCountCheck();
            if (checkResult != null) {
                ReloadLogHelper.warn2(checkResult);
                service.writeErrMessage(ErrorCode.ER_DOING_DDL, checkResult);
                return;
            }
            try {
                if (OBsharding_DServer.getInstance().getConfig().isFullyConfigured()) {
                    final ReentrantReadWriteLock confLock = OBsharding_DServer.getInstance().getConfig().getLock();
                    confLock.readLock().lock();
                    ReloadLogHelper.graceInfo("added configLock");
                    try {
                        if (!ReloadManager.startReload(TRIGGER_TYPE_COMMAND, ConfStatus.Status.RELOAD_META)) {
                            service.writeErrMessage(ErrorCode.ER_UNKNOWN_ERROR, "Reload status error ,other client or cluster may in reload");
                            return;
                        }
                        if (ProxyMeta.getInstance().reloadMetaData(OBsharding_DServer.getInstance().getConfig(), filter)) {
                            msg = "reload metadata success";
                        } else {
                            interrupt = true;
                            isOK = false;
                            msg = "reload metadata interrupted by manager command";
                        }
                    } finally {
                        confLock.readLock().unlock();
                        ReloadLogHelper.briefInfo("released configLock");
                    }
                }
            } catch (Exception e) {
                isOK = false;
                msg = "reload metadata failed," + e.toString();
            }
        } finally {
            lock.unlock();
            ReloadLogHelper.briefInfo("released metaLock");
        }
        ReloadManager.reloadFinish();
        if (isOK) {
            LOGGER.info(msg);
            OkPacket ok = new OkPacket();
            ok.setPacketId(1);
            ok.setAffectedRows(1);
            ok.setServerStatus(2);
            ok.setMessage(msg.getBytes());
            ok.write(service.getConnection());
        } else {
            LOGGER.warn(msg);
            service.writeErrMessage(interrupt ? ErrorCode.ER_RELOAD_INTERRUPUTED : ErrorCode.ER_UNKNOWN_ERROR, msg);
        }
    }

    /**
     * get schemas and tables in where condition
     *
     * @param whereCondition
     * @return
     */
    private static Map<String, Set<String>> getFilter(String whereCondition) {
        Map<String, Set<String>> filter = new HashMap<>();
        Matcher matcher = PATTERN_WHERE.matcher(whereCondition);
        if (matcher.matches()) {
            Set<String> tables = null;
            if (!StringUtil.isEmpty(matcher.group(6))) {
                tables = new HashSet<>(1);
                tables.add(StringUtil.removeAllApostrophe(matcher.group(6)));
            }
            filter.put(StringUtil.removeAllApostrophe(matcher.group(1)), tables);
        }
        matcher = PATTERN_IN.matcher(whereCondition);
        if (matcher.matches()) {
            String schemaTable = matcher.group(1);
            for (String s : schemaTable.split(",")) {
                String temp = s.replaceAll("'", "");
                Set<String> tables = filter.computeIfAbsent(temp.split("\\.")[0], k -> new HashSet<>());
                tables.add(temp.split("\\.")[1]);
            }
        }
        return filter;
    }
}
