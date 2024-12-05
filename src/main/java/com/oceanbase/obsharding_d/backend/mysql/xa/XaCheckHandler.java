/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.xa;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.datasource.ShardingNode;
import com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction.xa.XAAnalysisHandler;
import com.oceanbase.obsharding_d.backend.mysql.xa.recovery.Repository;
import com.oceanbase.obsharding_d.backend.mysql.xa.recovery.impl.FileSystemRepository;
import com.oceanbase.obsharding_d.backend.mysql.xa.recovery.impl.KVStoreRepository;
import com.oceanbase.obsharding_d.config.model.ClusterConfig;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.config.model.sharding.SchemaConfig;
import com.oceanbase.obsharding_d.config.model.sharding.table.BaseTableConfig;
import com.oceanbase.obsharding_d.singleton.Scheduler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public final class XaCheckHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(XaCheckHandler.class);

    private static final XaCheckHandler INSTANCE = new XaCheckHandler();
    private ScheduledFuture scheduledFuture;
    private long xaIdCheckPeriod; // s

    private XaCheckHandler() {
        xaIdCheckPeriod = SystemConfig.getInstance().getXaIdCheckPeriod();
    }

    // XA recovery log check
    public static void performXARecoveryLog() {
        // fetch the recovery log
        CoordinatorLogEntry[] coordinatorLogEntries = getCoordinatorLogEntries();
        // init into in memory cached
        for (CoordinatorLogEntry coordinatorLogEntry1 : coordinatorLogEntries) {
            OBsharding_DServer.getInstance().genXidSeq(coordinatorLogEntry1.getId());
            XAStateLog.flushMemoryRepository(coordinatorLogEntry1.getId(), coordinatorLogEntry1);
        }
        for (CoordinatorLogEntry coordinatorLogEntry : coordinatorLogEntries) {
            boolean needRollback = false;
            boolean needCommit = false;
            if (coordinatorLogEntry.getTxState() == TxState.TX_COMMIT_FAILED_STATE ||
                    // will committing, may send but failed receiving, should commit agagin
                    coordinatorLogEntry.getTxState() == TxState.TX_COMMITTING_STATE) {
                needCommit = true;
            } else if (coordinatorLogEntry.getTxState() == TxState.TX_ROLLBACK_FAILED_STATE ||
                    //don't konw prepare is succeed or not ,should rollback
                    coordinatorLogEntry.getTxState() == TxState.TX_PREPARE_UNCONNECT_STATE ||
                    // will rollbacking, may send but failed receiving,should rollback again
                    coordinatorLogEntry.getTxState() == TxState.TX_ROLLBACKING_STATE ||
                    // will preparing, may send but failed receiving,should rollback again
                    coordinatorLogEntry.getTxState() == TxState.TX_PREPARING_STATE) {
                needRollback = true;

            }
            if (needCommit || needRollback) {
                tryRecovery(coordinatorLogEntry, needCommit);
            }
        }
    }

    public static void checkResidualXA() {
        (new XAAnalysisHandler()).
                checkResidualByStartup();
    }

    private static void tryRecovery(CoordinatorLogEntry coordinatorLogEntry, boolean needCommit) {
        StringBuilder xaCmd = new StringBuilder();
        if (needCommit) {
            xaCmd.append("XA COMMIT ");
        } else {
            xaCmd.append("XA ROLLBACK ");
        }
        for (int j = 0; j < coordinatorLogEntry.getParticipants().length; j++) {
            ParticipantLogEntry participantLogEntry = coordinatorLogEntry.getParticipants()[j];
            // XA commit
            if (participantLogEntry.getTxState() != TxState.TX_COMMIT_FAILED_STATE &&
                    participantLogEntry.getTxState() != TxState.TX_COMMITTING_STATE &&
                    participantLogEntry.getTxState() != TxState.TX_PREPARE_UNCONNECT_STATE &&
                    participantLogEntry.getTxState() != TxState.TX_ROLLBACKING_STATE &&
                    participantLogEntry.getTxState() != TxState.TX_ROLLBACK_FAILED_STATE &&
                    participantLogEntry.getTxState() != TxState.TX_ENDED_STATE &&
                    participantLogEntry.getTxState() != TxState.TX_PREPARED_STATE &&
                    participantLogEntry.getTxState() != TxState.TX_PREPARING_STATE) {
                continue;
            }
            outLoop:
            for (SchemaConfig schema : OBsharding_DServer.getInstance().getConfig().getSchemas().values()) {
                for (BaseTableConfig table : schema.getTables().values()) {
                    for (String shardingNode : table.getShardingNodes()) {
                        ShardingNode dn = OBsharding_DServer.getInstance().getConfig().getShardingNodes().get(shardingNode);
                        if (participantLogEntry.compareAddress(dn.getDbGroup().getWriteDbInstance().getConfig().getIp(), dn.getDbGroup().getWriteDbInstance().getConfig().getPort(), dn.getDatabase())) {
                            appendXACmd(coordinatorLogEntry, dn, xaCmd, participantLogEntry);
                            XAAnalysisHandler xaAnalysisHandler = new XAAnalysisHandler(dn.getDbGroup().getWriteDbInstance());
                            xaAnalysisHandler.executeXaCmd(xaCmd.toString(), needCommit, participantLogEntry);
                            if (!xaAnalysisHandler.isSuccess()) {
                                throw new RuntimeException("Fail to recover xa when OBsharding-D start, please check backend mysql.");
                            }

                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(String.format("[%s] Host:[%s] sharding:[%s]", xaCmd, dn.getName(), dn.getDatabase()));
                            }

                            //reset xaCmd
                            xaCmd.setLength(0);
                            if (needCommit) {
                                xaCmd.append("XA COMMIT ");
                            } else {
                                xaCmd.append("XA ROLLBACK ");
                            }
                            break outLoop;
                        }
                    }
                }
            }
        }
        XAStateLog.saveXARecoveryLog(coordinatorLogEntry.getId(), needCommit ? TxState.TX_COMMITTED_STATE : TxState.TX_ROLLBACKED_STATE);
        XAStateLog.writeCheckpoint(coordinatorLogEntry.getId());
    }

    private static void appendXACmd(CoordinatorLogEntry coordinatorLogEntry, ShardingNode dn, StringBuilder xaCmd, ParticipantLogEntry participantLogEntry) {
        xaCmd.append(coordinatorLogEntry.getId(), 0, coordinatorLogEntry.getId().length() - 1);
        xaCmd.append(".");
        xaCmd.append(dn.getDatabase());
        if (participantLogEntry.getExpires() != 0) {
            xaCmd.append(".");
            xaCmd.append(participantLogEntry.getExpires());
        }
        xaCmd.append("'");
    }

    // covert the collection to array
    private static CoordinatorLogEntry[] getCoordinatorLogEntries() {
        Repository fileRepository = ClusterConfig.getInstance().isClusterEnable() && ClusterConfig.getInstance().useZkMode() ? new KVStoreRepository() : new FileSystemRepository();
        Collection<CoordinatorLogEntry> allCoordinatorLogEntries = fileRepository.getAllCoordinatorLogEntries(true);
        fileRepository.close();
        if (allCoordinatorLogEntries == null) {
            return new CoordinatorLogEntry[0];
        }
        if (allCoordinatorLogEntries.size() == 0) {
            return new CoordinatorLogEntry[0];
        }
        return allCoordinatorLogEntries.toArray(new CoordinatorLogEntry[allCoordinatorLogEntries.size()]);
    }

    // XaId Check Period
    public static void initXaIdCheckPeriod() {
        startXaIdCheckPeriod();
    }

    public static void adjustXaIdCheckPeriod(long period0) {
        synchronized (INSTANCE) {
            if (period0 != INSTANCE.xaIdCheckPeriod) {
                INSTANCE.xaIdCheckPeriod = period0;
                if (INSTANCE.scheduledFuture != null)
                    stopXaIdCheckPeriod();
                if (period0 > 0) {
                    startXaIdCheckPeriod();
                }
            }
        }
    }

    private static void startXaIdCheckPeriod() {
        synchronized (INSTANCE) {
            if (INSTANCE.xaIdCheckPeriod > 0) {
                INSTANCE.scheduledFuture = Scheduler.getInstance().getScheduledExecutor().scheduleWithFixedDelay(() -> {
                    (new XAAnalysisHandler()).checkResidualTask();
                }, 0, INSTANCE.xaIdCheckPeriod, TimeUnit.SECONDS);
                LOGGER.info("====================================Start XaIdCheckPeriod[{}]=========================================", INSTANCE.xaIdCheckPeriod);
            }
        }
    }

    private static void stopXaIdCheckPeriod() {
        synchronized (INSTANCE) {
            ScheduledFuture future = INSTANCE.scheduledFuture;
            if (future != null) {
                if (!future.isCancelled()) {
                    future.cancel(false);
                }
                INSTANCE.scheduledFuture = null;
                LOGGER.info("====================================Stop XaIdCheckPeriod=========================================");
            }
        }
    }

    public static long getXaIdCheckPeriod() {
        return INSTANCE.xaIdCheckPeriod;
    }
}
