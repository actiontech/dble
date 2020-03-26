/*
 * Copyright (C) 2016-2020 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble;

import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.singleton.CustomMySQLHa;
import com.actiontech.dble.backend.datasource.PhysicalDataHost;
import com.actiontech.dble.backend.datasource.PhysicalDataNode;
import com.actiontech.dble.backend.mysql.xa.*;
import com.actiontech.dble.backend.mysql.xa.recovery.Repository;
import com.actiontech.dble.backend.mysql.xa.recovery.impl.FileSystemRepository;
import com.actiontech.dble.backend.mysql.xa.recovery.impl.KVStoreRepository;
import com.actiontech.dble.buffer.DirectByteBufferPool;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.log.transaction.TxnLogProcessor;
import com.actiontech.dble.manager.ManagerConnectionFactory;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.net.*;
import com.actiontech.dble.net.handler.*;
import com.actiontech.dble.net.mysql.WriteToBackendTask;
import com.actiontech.dble.server.ServerConnectionFactory;
import com.actiontech.dble.server.status.SlowQueryLog;
import com.actiontech.dble.server.variables.SystemVariables;
import com.actiontech.dble.server.variables.VarsExtractorHandler;
import com.actiontech.dble.singleton.*;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;
import com.actiontech.dble.util.ExecutorUtil;
import com.actiontech.dble.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.AsynchronousChannelGroup;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mycat
 */
public final class DbleServer {

    public static final String NAME = "Dble_";

    private static final DbleServer INSTANCE = new DbleServer();
    private static final Logger LOGGER = LoggerFactory.getLogger("Server");
    //used by manager command show @@binlog_status to get a stable GTID
    private final AtomicBoolean backupLocked = new AtomicBoolean(false);


    private volatile SystemVariables systemVariables = new SystemVariables();
    private TxnLogProcessor txnLogProcessor;

    private AsynchronousChannelGroup[] asyncChannelGroups;
    private AtomicInteger channelIndex = new AtomicInteger();

    private volatile int nextFrontProcessor;
    private volatile int nextBackendProcessor;

    private boolean aio = false;

    private final AtomicLong xaIDInc = new AtomicLong();

    public static DbleServer getInstance() {
        return INSTANCE;
    }

    private ServerConfig config;
    //dble server on/offline flag
    private final AtomicBoolean isOnline = new AtomicBoolean(true);
    private long startupTime;
    private NIOProcessor[] frontProcessors;
    private NIOProcessor[] backendProcessors;
    private SocketConnector connector;
    private ExecutorService businessExecutor;
    private ExecutorService backendBusinessExecutor;
    private ExecutorService writeToBackendExecutor;
    private ExecutorService complexQueryExecutor;
    private ExecutorService timerExecutor;
    private Map<String, ThreadWorkUsage> threadUsedMap = new ConcurrentHashMap<>();
    private BlockingQueue<FrontendCommandHandler> frontHandlerQueue;
    private BlockingQueue<List<WriteToBackendTask>> writeToBackendQueue;
    private Queue<FrontendCommandHandler> concurrentFrontHandlerQueue;
    private Queue<BackendAsyncHandler> concurrentBackHandlerQueue;

    private DbleServer() {
    }


    public void startup() throws Exception {
        LOGGER.info("===========================================DBLE SERVER STARTING===================================");
        this.config = new ServerConfig();
        this.startupTime = TimeUtil.currentTimeMillis();
        LOGGER.info("=========================================Config file read finish==================================");
        SystemConfig system = config.getSystem();
        if (system.isUseOuterHa()) {
            LOGGER.info("=========================================Init Outter Ha Config==================================");
            HaConfigManager.getInstance().init();
        } else if (ClusterHelper.useClusterHa()) {
            throw new Exception("useOuterHa can not be false when useClusterHa in myid is true");
        }
        if (system.getEnableAlert() == 1) {
            AlertUtil.switchAlert(true);
        }
        AlertManager.getInstance().startAlert();
        LOGGER.info("========================================Alert Manager start finish================================");

        // server startup
        LOGGER.info("============================================Server start params===================================");
        LOGGER.info(NAME + "Server is ready to startup ...");
        String inf = "Startup processors ...,total processors:" +
                system.getProcessors() + ",aio thread pool size:" +
                system.getProcessorExecutor() +
                "    \r\n each process allocated socket buffer pool " +
                " bytes ,a page size:" +
                system.getBufferPoolPageSize() +
                "  a page's chunk number(PageSize/ChunkSize) is:" +
                (system.getBufferPoolPageSize() / system.getBufferPoolChunkSize()) +
                "  buffer page's number is:" +
                system.getBufferPoolPageNumber();
        LOGGER.info(inf);
        LOGGER.info("sysconfig params:" + system.toString());

        aio = (system.getUsingAIO() == 1);

        LOGGER.info("===========================================Init bufferPool start==================================");
        BufferPoolManager.getInstance().init(system);
        LOGGER.info("===========================================Init bufferPool finish=================================");

        // startup processors
        int frontProcessorCount = system.getProcessors();
        int backendProcessorCount = system.getBackendProcessors();
        frontProcessors = new NIOProcessor[frontProcessorCount];
        backendProcessors = new NIOProcessor[backendProcessorCount];


        businessExecutor = ExecutorUtil.createFixed("BusinessExecutor", system.getProcessorExecutor());
        backendBusinessExecutor = ExecutorUtil.createFixed("backendBusinessExecutor", system.getBackendProcessorExecutor());
        writeToBackendExecutor = ExecutorUtil.createFixed("writeToBackendExecutor", system.getWriteToBackendExecutor());
        complexQueryExecutor = ExecutorUtil.createCached("complexQueryExecutor", system.getComplexExecutor());
        timerExecutor = ExecutorUtil.createFixed("Timer", 1);

        LOGGER.info("====================================Task Queue&Thread init start==================================");
        initTaskQueue(system);
        LOGGER.info("==================================Task Queue&Thread init finish===================================");


        for (int i = 0; i < frontProcessorCount; i++) {
            frontProcessors[i] = new NIOProcessor("frontProcessor" + i, BufferPoolManager.getBufferPool());
        }
        for (int i = 0; i < backendProcessorCount; i++) {
            backendProcessors[i] = new NIOProcessor("backendProcessor" + i, BufferPoolManager.getBufferPool());
        }

        if (system.getEnableSlowLog() == 1) {
            SlowQueryLog.getInstance().setEnableSlowLog(true);
        }

        LOGGER.info("==============================Connection  Connector&Acceptor init start===========================");
        // startup manager
        SocketAcceptor manager;
        SocketAcceptor server;
        if (aio) {
            int processorCount = frontProcessorCount + backendProcessorCount;
            LOGGER.info("using aio network handler ");
            asyncChannelGroups = new AsynchronousChannelGroup[processorCount];
            initAioProecssor(processorCount);

            connector = new AIOConnector();
            manager = new AIOAcceptor(NAME + "Manager", system.getBindIp(),
                    system.getManagerPort(), 100, new ManagerConnectionFactory(), this.asyncChannelGroups[0]);
            server = new AIOAcceptor(NAME + "Server", system.getBindIp(),
                    system.getServerPort(), system.getServerBacklog(), new ServerConnectionFactory(), this.asyncChannelGroups[0]);

        } else {
            NIOReactorPool frontReactorPool = new NIOReactorPool(
                    DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + "NIO_REACTOR_FRONT",
                    frontProcessorCount);
            NIOReactorPool backendReactorPool = new NIOReactorPool(
                    DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + "NIO_REACTOR_BACKEND",
                    backendProcessorCount);

            connector = new NIOConnector(DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + "NIOConnector", backendReactorPool);
            ((NIOConnector) connector).start();

            manager = new NIOAcceptor(DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + NAME + "Manager", system.getBindIp(),
                    system.getManagerPort(), 100, new ManagerConnectionFactory(), frontReactorPool);
            server = new NIOAcceptor(DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + NAME + "Server", system.getBindIp(),
                    system.getServerPort(), system.getServerBacklog(), new ServerConnectionFactory(), frontReactorPool);
        }
        LOGGER.info("==========================Connection Connector&Acceptor init finish===============================");

        this.config.testConnection();
        LOGGER.info("==========================================Test connection finish==================================");

        // sync global status
        this.config.getAndSyncKeyVariables();
        LOGGER.info("=====================================Get And Sync KeyVariables finish=============================");

        // start transaction SQL log
        if (config.getSystem().getRecordTxn() == 1) {
            txnLogProcessor = new TxnLogProcessor();
            txnLogProcessor.setName("TxnLogProcessor");
            txnLogProcessor.start();
        }

        SequenceManager.init(config.getSystem().getSequnceHandlerType());
        LOGGER.info("===================================Sequence manager init finish===================================");


        LOGGER.info("==============================Pull metaData from MySQL start======================================");
        pullVarAndMeta();
        LOGGER.info("==============================Pull metaData from MySQL finish=====================================");

        FrontendUserManager.getInstance().initForLatest(config.getUsers(), system.getMaxCon());


        if (ClusterGeneralConfig.isUseGeneralCluster()) {
            LOGGER.info("===================Init online status in cluster==================");
            try {
                OnlineStatus.getInstance().mainThreadInitClusterOnline();
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                LOGGER.warn("cluster can not connection ", e);
            }
        }


        CacheService.getInstance().init(this.systemVariables.isLowerCaseTableNames());
        LOGGER.info("====================================Cache service init finish=====================================");

        LOGGER.info("=====================================Perform XA recovery log======================================");
        performXARecoveryLog();
        LOGGER.info("====================================Perform XA recovery finish====================================");

        manager.start();
        LOGGER.info(manager.getName() + " is started and listening on " + manager.getPort());
        server.start();
        LOGGER.info(server.getName() + " is started and listening on " + server.getPort());
        LOGGER.info("=====================================Server started success=======================================");

        Scheduler.getInstance().init(system, timerExecutor);
        LOGGER.info("=======================================Scheduler started==========================================");

        CronScheduler.getInstance().init(config.getSchemas());
        LOGGER.info("====================================CronScheduler started=========================================");

        CustomMySQLHa.getInstance().start();
        LOGGER.info("======================================ALL START INIT FINISH=======================================");
    }

    private void initAioProecssor(int processorCount) throws IOException {
        for (int i = 0; i < processorCount; i++) {
            asyncChannelGroups[i] = AsynchronousChannelGroup.withFixedThreadPool(processorCount,
                    new ThreadFactory() {
                        private int inx = 1;

                        @Override
                        public Thread newThread(Runnable r) {
                            Thread th = new Thread(r);
                            //TODO
                            th.setName(DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + "AIO" + (inx++));
                            LOGGER.info("created new AIO thread " + th.getName());
                            return th;
                        }
                    }
            );
        }
    }

    private void initTaskQueue(SystemConfig system) {
        if (system.getUsePerformanceMode() == 1) {
            concurrentFrontHandlerQueue = new ConcurrentLinkedQueue<>();
            for (int i = 0; i < system.getProcessorExecutor(); i++) {
                businessExecutor.execute(new ConcurrentFrontEndHandlerRunnable(concurrentFrontHandlerQueue));
            }

            concurrentBackHandlerQueue = new ConcurrentLinkedQueue<>();
            for (int i = 0; i < system.getBackendProcessorExecutor(); i++) {
                backendBusinessExecutor.execute(new ConcurrentBackEndHandlerRunnable(concurrentBackHandlerQueue));
            }
        } else {
            frontHandlerQueue = new LinkedBlockingQueue<>();
            for (int i = 0; i < system.getProcessorExecutor(); i++) {
                businessExecutor.execute(new FrontEndHandlerRunnable(frontHandlerQueue));
            }
        }

        writeToBackendQueue = new LinkedBlockingQueue<>();
        for (int i = 0; i < system.getWriteToBackendExecutor(); i++) {
            writeToBackendExecutor.execute(new WriteToBackendRunnable(writeToBackendQueue));
        }
    }


    private void initDataHost() {
        // init datahost
        Map<String, PhysicalDataHost> dataHosts = this.getConfig().getDataHosts();
        LOGGER.info("Initialize dataHost ...");
        for (PhysicalDataHost node : dataHosts.values()) {
            node.init();
            node.startHeartbeat();
        }
    }

    public void reloadSystemVariables(SystemVariables sys) {
        systemVariables = sys;
    }


    public boolean isUseOuterHa() {
        return config.getSystem().isUseOuterHa();
    }

    public NIOProcessor nextFrontProcessor() {
        int i = ++nextFrontProcessor;
        if (i >= frontProcessors.length) {
            i = nextFrontProcessor = 0;
        }
        return frontProcessors[i];
    }

    public NIOProcessor nextBackendProcessor() {
        int i = ++nextBackendProcessor;
        if (i >= backendProcessors.length) {
            i = nextBackendProcessor = 0;
        }
        return backendProcessors[i];
    }

    public BlockingQueue<List<WriteToBackendTask>> getWriteToBackendQueue() {
        return writeToBackendQueue;
    }

    public Map<String, ThreadWorkUsage> getThreadUsedMap() {
        return threadUsedMap;
    }

    public Queue<FrontendCommandHandler> getFrontHandlerQueue() {
        if (config.getSystem().getUsePerformanceMode() == 1) {
            return concurrentFrontHandlerQueue;
        } else {
            return frontHandlerQueue;
        }
    }


    // check the closed/overtime connection
    public Runnable processorCheck() {
        return new Runnable() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            for (NIOProcessor p : backendProcessors) {
                                p.checkBackendCons();
                            }
                        } catch (Exception e) {
                            LOGGER.info("checkBackendCons caught err:" + e);
                        }
                    }
                });
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            for (NIOProcessor p : frontProcessors) {
                                p.checkFrontCons();
                            }
                        } catch (Exception e) {
                            LOGGER.info("checkFrontCons caught err:" + e);
                        }
                    }
                });
            }
        };
    }


    private void reviseSchemas() {
        if (systemVariables.isLowerCaseTableNames()) {
            config.reviseLowerCase();
            ConfigUtil.setSchemasForPool(config.getDataHosts(), config.getDataNodes());
        } else {
            config.loadSequence();
            config.selfChecking0();
        }
    }

    private void pullVarAndMeta() throws IOException {
        ProxyMetaManager tmManager = new ProxyMetaManager();
        ProxyMeta.getInstance().setTmManager(tmManager);
        if (!this.getConfig().isDataHostWithoutWR()) {
            LOGGER.info("get variables Data start");
            //init for sys VAR
            VarsExtractorHandler handler = new VarsExtractorHandler(config.getDataHosts());
            SystemVariables newSystemVariables = handler.execute();
            if (newSystemVariables == null) {
                throw new IOException("Can't get variables from data node");
            } else {
                systemVariables = newSystemVariables;
            }
            reviseSchemas();
            initDataHost();
            LOGGER.info("get variables Data end");
            //init tmManager
            try {
                tmManager.init(this.getConfig());
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }


    // XA recovery log check
    private void performXARecoveryLog() {
        // fetch the recovery log
        CoordinatorLogEntry[] coordinatorLogEntries = getCoordinatorLogEntries();
        // init into in memory cached
        for (CoordinatorLogEntry coordinatorLogEntry1 : coordinatorLogEntries) {
            genXidSeq(coordinatorLogEntry1.getId());
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

    private void tryRecovery(CoordinatorLogEntry coordinatorLogEntry, boolean needCommit) {
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
                for (TableConfig table : schema.getTables().values()) {
                    for (String dataNode : table.getDataNodes()) {
                        PhysicalDataNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(dataNode);
                        if (participantLogEntry.compareAddress(dn.getDataHost().getWriteSource().getConfig().getIp(), dn.getDataHost().getWriteSource().getConfig().getPort(), dn.getDatabase())) {
                            xaCmd.append(coordinatorLogEntry.getId().substring(0, coordinatorLogEntry.getId().length() - 1));
                            xaCmd.append(".");
                            xaCmd.append(dn.getDatabase());
                            if (participantLogEntry.getExpires() != 0) {
                                xaCmd.append(".");
                                xaCmd.append(participantLogEntry.getExpires());
                            }
                            xaCmd.append("'");
                            XARecoverHandler handler = new XARecoverHandler(needCommit, participantLogEntry);
                            handler.execute(xaCmd.toString(), dn.getDatabase(), dn.getDataHost().getWriteSource());
                            if (!handler.isSuccess()) {
                                throw new RuntimeException("Fail to recover xa when dble start, please check backend mysql.");
                            }

                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(String.format("[%s] Host:[%s] schema:[%s]", xaCmd, dn.getName(), dn.getDatabase()));
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

    /**
     * covert the collection to array
     **/
    private CoordinatorLogEntry[] getCoordinatorLogEntries() {
        Repository fileRepository = ClusterGeneralConfig.isUseZK() ? new KVStoreRepository() : new FileSystemRepository();
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

    public String genXaTxId() {
        long seq = this.xaIDInc.incrementAndGet();
        if (seq < 0) {
            synchronized (xaIDInc) {
                if (xaIDInc.get() < 0) {
                    xaIDInc.set(0);
                }
                seq = xaIDInc.incrementAndGet();
            }
        }
        StringBuilder id = new StringBuilder();
        id.append("'" + NAME + "Server.");
        if (ClusterGeneralConfig.isUseZK()) {
            id.append(ZkConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID));
        } else if (ClusterGeneralConfig.isUseGeneralCluster()) {
            id.append(ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID));
        } else {
            id.append(this.getConfig().getSystem().getServerNodeId());

        }
        id.append(".");
        id.append(seq);
        id.append("'");
        return id.toString();
    }

    private void genXidSeq(String xaID) {
        String[] idSplit = xaID.replace("'", "").split("\\.");
        long seq = Long.parseLong(idSplit[2]);
        if (xaIDInc.get() < seq) {
            xaIDInc.set(seq);
        }
    }


    /**
     * get next AsynchronousChannel ,first is exclude if multi
     * AsynchronousChannelGroups
     *
     * @return AsynchronousChannelGroup
     */
    public AsynchronousChannelGroup getNextAsyncChannelGroup() {
        if (asyncChannelGroups.length == 1) {
            return asyncChannelGroups[0];
        } else {
            int index = (channelIndex.incrementAndGet()) % asyncChannelGroups.length;
            if (index == 0) {
                channelIndex.incrementAndGet();
                return asyncChannelGroups[1];
            } else {
                return asyncChannelGroups[index];
            }

        }
    }


    public boolean isAIO() {
        return aio;
    }


    public ExecutorService getTimerExecutor() {
        return timerExecutor;
    }


    public ExecutorService getComplexQueryExecutor() {
        return complexQueryExecutor;
    }

    public AtomicBoolean getBackupLocked() {
        return backupLocked;
    }

    public boolean isBackupLocked() {
        return backupLocked.get();
    }


    public ServerConfig getConfig() {
        return config;
    }


    public Queue<BackendAsyncHandler> getBackHandlerQueue() {
        return concurrentBackHandlerQueue;
    }

    public NIOProcessor[] getFrontProcessors() {
        return frontProcessors;
    }

    public NIOProcessor[] getBackendProcessors() {
        return backendProcessors;
    }

    public SocketConnector getConnector() {
        return connector;
    }


    public long getStartupTime() {
        return startupTime;
    }

    public boolean isOnline() {
        return isOnline.get();
    }

    public void offline() {
        isOnline.set(false);
    }

    public void online() {
        isOnline.set(true);
    }

    public SystemVariables getSystemVariables() {
        return systemVariables;
    }

    public TxnLogProcessor getTxnLogProcessor() {
        return txnLogProcessor;
    }

    public ExecutorService getBusinessExecutor() {
        return businessExecutor;
    }

    public ExecutorService getWriteToBackendExecutor() {
        return writeToBackendExecutor;
    }

    public ExecutorService getBackendBusinessExecutor() {
        return backendBusinessExecutor;
    }

}
