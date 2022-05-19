package com.actiontech.dble.backend.mysql.xa;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.datasource.ShardingNode;
import com.actiontech.dble.backend.mysql.nio.handler.transaction.xa.XAAnalysisHandler;
import com.actiontech.dble.backend.mysql.xa.recovery.Repository;
import com.actiontech.dble.backend.mysql.xa.recovery.impl.FileSystemRepository;
import com.actiontech.dble.backend.mysql.xa.recovery.impl.KVStoreRepository;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.sharding.SchemaConfig;
import com.actiontech.dble.config.model.sharding.table.BaseTableConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

public final class XaCheckHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(XaCheckHandler.class);

    private XaCheckHandler() {
    }

    // XA recovery log check
    public static void performXARecoveryLog() {
        // fetch the recovery log
        CoordinatorLogEntry[] coordinatorLogEntries = getCoordinatorLogEntries();
        // init into in memory cached
        for (CoordinatorLogEntry coordinatorLogEntry1 : coordinatorLogEntries) {
            DbleServer.getInstance().genXidSeq(coordinatorLogEntry1.getId());
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
            for (SchemaConfig schema : DbleServer.getInstance().getConfig().getSchemas().values()) {
                for (BaseTableConfig table : schema.getTables().values()) {
                    for (String shardingNode : table.getShardingNodes()) {
                        ShardingNode dn = DbleServer.getInstance().getConfig().getShardingNodes().get(shardingNode);
                        if (participantLogEntry.compareAddress(dn.getDbGroup().getWriteDbInstance().getConfig().getIp(), dn.getDbGroup().getWriteDbInstance().getConfig().getPort(), dn.getDatabase())) {
                            appendXACmd(coordinatorLogEntry, dn, xaCmd, participantLogEntry);
                            XAAnalysisHandler xaAnalysisHandler = new XAAnalysisHandler(dn.getDbGroup().getWriteDbInstance());
                            xaAnalysisHandler.executeXaCmd(xaCmd.toString(), needCommit, participantLogEntry);
                            if (!xaAnalysisHandler.isSuccess()) {
                                throw new RuntimeException("Fail to recover xa when dble start, please check backend mysql.");
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
}
