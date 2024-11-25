/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d;

import com.oceanbase.obsharding_d.alarm.AlertUtil;
import com.oceanbase.obsharding_d.backend.datasource.PhysicalDbGroup;
import com.oceanbase.obsharding_d.backend.mysql.xa.XaCheckHandler;
import com.oceanbase.obsharding_d.buffer.DirectByteBufferPool;
import com.oceanbase.obsharding_d.cluster.zkprocess.zktoxml.ZktoXmlMain;
import com.oceanbase.obsharding_d.config.OBsharding_DTempConfig;
import com.oceanbase.obsharding_d.config.ServerConfig;
import com.oceanbase.obsharding_d.config.model.ClusterConfig;
import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.config.util.ConfigUtil;
import com.oceanbase.obsharding_d.log.general.GeneralLogProcessor;
import com.oceanbase.obsharding_d.log.sqldump.SqlDumpLogHelper;
import com.oceanbase.obsharding_d.log.transaction.TxnLogProcessor;
import com.oceanbase.obsharding_d.meta.ProxyMetaManager;
import com.oceanbase.obsharding_d.net.OBsharding_DSocketOptions;
import com.oceanbase.obsharding_d.net.IOProcessor;
import com.oceanbase.obsharding_d.net.SocketAcceptor;
import com.oceanbase.obsharding_d.net.SocketConnector;
import com.oceanbase.obsharding_d.net.connection.AbstractConnection;
import com.oceanbase.obsharding_d.net.executor.BackendCurrentRunnable;
import com.oceanbase.obsharding_d.net.executor.FrontendBlockRunnable;
import com.oceanbase.obsharding_d.net.executor.FrontendCurrentRunnable;
import com.oceanbase.obsharding_d.net.executor.WriteToBackendRunnable;
import com.oceanbase.obsharding_d.net.impl.aio.AIOAcceptor;
import com.oceanbase.obsharding_d.net.impl.aio.AIOConnector;
import com.oceanbase.obsharding_d.net.impl.nio.NIOAcceptor;
import com.oceanbase.obsharding_d.net.impl.nio.NIOConnector;
import com.oceanbase.obsharding_d.net.impl.nio.RW;
import com.oceanbase.obsharding_d.net.mysql.WriteToBackendTask;
import com.oceanbase.obsharding_d.net.service.ServiceTask;
import com.oceanbase.obsharding_d.net.ssl.SSLWrapperRegistry;
import com.oceanbase.obsharding_d.server.status.SlowQueryLog;
import com.oceanbase.obsharding_d.server.variables.SystemVariables;
import com.oceanbase.obsharding_d.server.variables.VarsExtractorHandler;
import com.oceanbase.obsharding_d.services.factorys.ManagerConnectionFactory;
import com.oceanbase.obsharding_d.services.factorys.ServerConnectionFactory;
import com.oceanbase.obsharding_d.singleton.*;
import com.oceanbase.obsharding_d.statistic.sql.StatisticManager;
import com.oceanbase.obsharding_d.statistic.stat.ThreadWorkUsage;
import com.oceanbase.obsharding_d.util.ExecutorUtil;
import com.oceanbase.obsharding_d.util.TimeUtil;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.AsynchronousChannelGroup;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public final class OBsharding_DServer {

    public static final String NAME = "OBsharding-D_";

    private static final OBsharding_DServer INSTANCE = new OBsharding_DServer();
    private static final Logger LOGGER = LoggerFactory.getLogger("Server");
    //used by manager command show @@binlog_status to get a stable GTID
    private final AtomicBoolean backupLocked = new AtomicBoolean(false);

    public static final String FRONT_WORKER_NAME = "frontWorker";
    public static final String BACKEND_WORKER_NAME = "backendWorker";
    public static final String WRITE_TO_BACKEND_WORKER_NAME = "writeToBackendWorker";
    public static final String COMPLEX_QUERY_EXECUTOR_NAME = "complexQueryWorker";
    public static final String TIMER_WORKER_NAME = "Timer";
    public static final String NIO_FRONT_RW = "NIOFrontRW";
    public static final String NIO_BACKEND_RW = "NIOBackendRW";
    public static final String AIO_EXECUTOR_NAME = "AIO";

    private volatile SystemVariables systemVariables = new SystemVariables();
    private TxnLogProcessor txnLogProcessor;
    private GeneralLogProcessor generalLogProcessor;

    private AsynchronousChannelGroup[] asyncChannelGroups;
    private AtomicInteger channelIndex = new AtomicInteger();

    private volatile int nextFrontProcessor;
    private volatile int nextBackendProcessor;

    private boolean aio = false;

    private final AtomicLong xaIDInc = new AtomicLong();

    public static OBsharding_DServer getInstance() {
        return INSTANCE;
    }

    private ServerConfig config;
    //OBsharding-D server on/offline flag
    private final AtomicBoolean isOnline = new AtomicBoolean(true);
    private long startupTime;
    private IOProcessor[] frontProcessors;
    private IOProcessor[] backendProcessors;
    private SocketConnector connector;
    private ExecutorService nioFrontExecutor;
    private ExecutorService nioBackendExecutor;
    private ExecutorService frontExecutor;
    private ExecutorService backendExecutor;
    private ExecutorService writeToBackendExecutor;
    private ExecutorService complexQueryExecutor;
    private ExecutorService timerExecutor;
    private Map<String, ThreadWorkUsage> threadUsedMap = new ConcurrentHashMap<>();

    private Deque<ServiceTask> frontHandlerQueue;
    private BlockingQueue<List<WriteToBackendTask>> writeToBackendQueue;

    private Queue<ServiceTask> concurrentBackHandlerQueue;
    private ConcurrentLinkedQueue<AbstractConnection> frontRegisterQueue;
    private ConcurrentLinkedQueue<AbstractConnection> backendRegisterQueue;

    private volatile boolean startup = false;
    private Map<String, Map<Thread, Runnable>> runnableMap = Maps.newConcurrentMap();

    private OBsharding_DServer() {
    }


    public void startup() throws Exception {
        LOGGER.info("===========================================OBsharding-D SERVER STARTING===================================");
        initServerConfig();
        this.startupTime = TimeUtil.currentTimeMillis();
        LOGGER.info("=========================================Config file read finish==================================");

        LOGGER.info("=========================================Init Outer Ha Config==================================");
        HaConfigManager.getInstance().init(false);

        if (SystemConfig.getInstance().getEnableAlert() == 1) {
            AlertUtil.switchAlert(true);
        }
        AlertManager.getInstance().startAlert();
        RoutePenetrationManager.getInstance().init();
        LOGGER.info("========================================Alert Manager start finish================================");

        // server startup
        LOGGER.info("============================================Server start params===================================");
        LOGGER.info(NAME + "Server is ready to startup ...");

        LOGGER.info("system config params:" + SystemConfig.getInstance().toString());

        LOGGER.info("===========================================Init bufferPool start==================================");
        String inf = "Buffer pool info:[The count of pages is:" +
                SystemConfig.getInstance().getBufferPoolPageNumber() + ",every page size:" +
                SystemConfig.getInstance().getBufferPoolPageSize() +
                ", every page's chunk number(PageSize/ChunkSize) is:" +
                (SystemConfig.getInstance().getBufferPoolPageSize() / SystemConfig.getInstance().getBufferPoolChunkSize()) +
                "]";
        LOGGER.info(inf);
        BufferPoolManager.getInstance().init();
        LOGGER.info("===========================================Init bufferPool finish=================================");

        // startup processors
        int frontProcessorCount = SystemConfig.getInstance().getNIOFrontRW();
        int backendProcessorCount = SystemConfig.getInstance().getNIOBackendRW();
        frontProcessors = new IOProcessor[frontProcessorCount];
        backendProcessors = new IOProcessor[backendProcessorCount];


        initExecutor(frontProcessorCount, backendProcessorCount);

        LOGGER.info("====================================Task Queue&Thread init start==================================");
        initTaskQueue();
        LOGGER.info("==================================Task Queue&Thread init finish===================================");


        for (int i = 0; i < frontProcessorCount; i++) {
            frontProcessors[i] = new IOProcessor("frontProcessor" + i, BufferPoolManager.getBufferPool());
        }
        for (int i = 0; i < backendProcessorCount; i++) {
            backendProcessors[i] = new IOProcessor("backendProcessor" + i, BufferPoolManager.getBufferPool());
        }

        if (SystemConfig.getInstance().getEnableSlowLog() == 1) {
            SlowQueryLog.getInstance().setEnableSlowLog(true);
        }

        if (SystemConfig.getInstance().getEnableStatistic() == 1 || SystemConfig.getInstance().getSamplingRate() > 0) {
            StatisticManager.getInstance().start();
        }

        LOGGER.info("==============================Connection  Connector&Acceptor init start===========================");
        // startup manager
        SocketAcceptor manager = null;
        SocketAcceptor server = null;

        aio = (SystemConfig.getInstance().getUsingAIO() == 1);
        if (aio) {
            int processorCount = frontProcessorCount + backendProcessorCount;
            LOGGER.info("using aio network handler ");
            asyncChannelGroups = new AsynchronousChannelGroup[processorCount];
            initAioProcessor(processorCount);

            connector = new AIOConnector();
            manager = new AIOAcceptor(NAME + "Manager", SystemConfig.getInstance().getBindIp(),
                    SystemConfig.getInstance().getManagerPort(), 100, new ManagerConnectionFactory(), this.asyncChannelGroups[0]);
            server = new AIOAcceptor(NAME + "Server", SystemConfig.getInstance().getBindIp(),
                    SystemConfig.getInstance().getServerPort(), SystemConfig.getInstance().getServerBacklog(), new ServerConnectionFactory(), this.asyncChannelGroups[0]);
        } else {
            for (int i = 0; i < frontProcessorCount; i++) {
                nioFrontExecutor.execute(new RW(frontRegisterQueue));
            }
            for (int i = 0; i < backendProcessorCount; i++) {
                nioBackendExecutor.execute(new RW(backendRegisterQueue));
            }

            connector = new NIOConnector(DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + "NIOConnector", backendRegisterQueue);
            connector.start();

            manager = new NIOAcceptor(DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + NAME + "Manager", SystemConfig.getInstance().getBindIp(),
                    SystemConfig.getInstance().getManagerPort(), 100, new ManagerConnectionFactory(), frontRegisterQueue);
            server = new NIOAcceptor(DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + NAME + "Server", SystemConfig.getInstance().getBindIp(),
                    SystemConfig.getInstance().getServerPort(), SystemConfig.getInstance().getServerBacklog(), new ServerConnectionFactory(), frontRegisterQueue);
        }
        LOGGER.info("==========================Connection Connector&Acceptor init finish===============================");

        SSLWrapperRegistry.init();
        FlowController.init();
        LOGGER.info("===================================flow controller finish===================================");

        this.config.testConnection();
        LOGGER.info("==========================================Test connection finish==================================");

        // sync global status
        this.config.getAndSyncKeyVariables();
        LOGGER.info("=====================================Get And Sync KeyVariables finish=============================");

        // start transaction SQL log
        if (SystemConfig.getInstance().getRecordTxn() == 1) {
            txnLogProcessor = new TxnLogProcessor();
            txnLogProcessor.setName("TxnLogProcessor");
            txnLogProcessor.start();
        }

        // general log
        generalLogProcessor = GeneralLogProcessor.getInstance();
        if (SystemConfig.getInstance().getEnableGeneralLog() == 1) {
            generalLogProcessor.start();
        }
        SqlDumpLogHelper.init();

        SequenceManager.init();
        LOGGER.info("===================================Sequence manager init finish===================================");

        LOGGER.info("==============================Pull metaData from MySQL start======================================");
        pullVarAndMeta();
        LOGGER.info("==============================Pull metaData from MySQL finish=====================================");


        LOGGER.info("=========================================Init online status in cluster==================================");
        initOnlineStatus();

        FrontendUserManager.getInstance().initForLatest(config.getUsers(), SystemConfig.getInstance().getMaxCon());

        CacheService.getInstance().init(this.systemVariables.isLowerCaseTableNames());
        LOGGER.info("====================================Cache service init finish=====================================");

        LOGGER.info("=====================================Perform XA recovery log======================================");
        XaCheckHandler.performXARecoveryLog();
        LOGGER.info("====================================Perform XA recovery finish====================================");

        LOGGER.info("====================================Check Residual XA====================================");
        XaCheckHandler.checkResidualXA();
        LOGGER.info("====================================Check Residual XA finish====================================");

        LOGGER.info("===================================Sync cluster pause status start====================================");
        PauseShardingNodeManager.getInstance().fetchClusterStatus();
        LOGGER.info("===================================Sync cluster pause status end  ====================================");

        // upload before the service start
        if (ClusterConfig.getInstance().isInitZkFirst()) {
            ZktoXmlMain.loadZkToFile();
            ProxyMeta.getInstance().getTmManager().init(this.getConfig());
            LOGGER.info("init file to Zk success");
        }

        manager.start();
        LOGGER.info(manager.getName() + " is started and listening on " + manager.getPort());
        server.start();
        LOGGER.info(server.getName() + " is started and listening on " + server.getPort());
        LOGGER.info("=====================================Server started success=======================================");

        Scheduler.getInstance().init(timerExecutor);
        LOGGER.info("=======================================Scheduler started==========================================");

        XaCheckHandler.initXaIdCheckPeriod();

        CronScheduler.getInstance().init(config.getSchemas());
        LOGGER.info("====================================CronScheduler started=========================================");

        CustomMySQLHa.getInstance().start();

        LOGGER.info("===========================================CHECK JDK VERSION===================================");
        checkJdkVersion();

        LOGGER.info("======================================ALL START INIT FINISH=======================================");
        startup = true;
    }

    private void checkJdkVersion() {
        OBsharding_DSocketOptions.getInstance().clean();
        if (OBsharding_DSocketOptions.osName().contains("Windows")) {
            LOGGER.warn("current system version does not support the tcpKeepIdle,tcpKeepInterval,tcpKeepCount parameter.");
            return;
        }
        if (!OBsharding_DSocketOptions.isKeepAliveOPTSupported()) {
            LOGGER.warn("current version is low and the tcpKeepIdle,tcpKeepInterval,tcpKeepCount parameter art not supported,please upgrade OracleJDK to version {}, upgrade OpenJDK to version {}.", OBsharding_DSocketOptions.ORACLE_VERSION, OBsharding_DSocketOptions.OPEN_VERSION);
        }
    }

    private void initExecutor(int frontProcessorCount, int backendProcessorCount) {
        frontExecutor = ExecutorUtil.createFixed(FRONT_WORKER_NAME, SystemConfig.getInstance().getFrontWorker(), runnableMap);
        backendExecutor = ExecutorUtil.createFixed(BACKEND_WORKER_NAME, SystemConfig.getInstance().getBackendWorker(), runnableMap);
        writeToBackendExecutor = ExecutorUtil.createFixed(WRITE_TO_BACKEND_WORKER_NAME, SystemConfig.getInstance().getWriteToBackendWorker(), runnableMap);
        complexQueryExecutor = ExecutorUtil.createCached(COMPLEX_QUERY_EXECUTOR_NAME, SystemConfig.getInstance().getComplexQueryWorker(), null);
        timerExecutor = ExecutorUtil.createFixed(TIMER_WORKER_NAME, 1);
        nioFrontExecutor = ExecutorUtil.createFixed(NIO_FRONT_RW, frontProcessorCount, runnableMap);
        nioBackendExecutor = ExecutorUtil.createFixed(NIO_BACKEND_RW, backendProcessorCount, runnableMap);
    }

    private void initServerConfig() throws Exception {
        //compatible with ZK first initialized
        if (ClusterConfig.getInstance().isClusterEnable() && !ClusterConfig.getInstance().isInitZkFirst()) {
            this.config = new ServerConfig(OBsharding_DTempConfig.getInstance().getUserConfig(), OBsharding_DTempConfig.getInstance().getDbConfig(),
                    OBsharding_DTempConfig.getInstance().getShardingConfig(), OBsharding_DTempConfig.getInstance().getSequenceConfig());
            OBsharding_DTempConfig.getInstance().clean();
            this.config.syncJsonToLocal(true);
        } else {
            this.config = new ServerConfig();
        }
    }

    private void initAioProcessor(int processorCount) throws IOException {
        for (int i = 0; i < processorCount; i++) {
            asyncChannelGroups[i] = AsynchronousChannelGroup.withFixedThreadPool(processorCount,
                    new ThreadFactory() {
                        private int inx = 1;

                        @Override
                        public Thread newThread(Runnable r) {
                            Thread th = new Thread(r);
                            th.setName(AIO_EXECUTOR_NAME + (inx++));
                            LOGGER.info("created new AIO thread " + th.getName());
                            return th;
                        }
                    }
            );
        }
    }

    private void initTaskQueue() {
        if (SystemConfig.getInstance().getUsePerformanceMode() == 1) {

            frontHandlerQueue = new ConcurrentLinkedDeque<>();
            for (int i = 0; i < SystemConfig.getInstance().getFrontWorker(); i++) {
                frontExecutor.execute(new FrontendCurrentRunnable(frontHandlerQueue));
            }

            concurrentBackHandlerQueue = new ConcurrentLinkedQueue<>();
            for (int i = 0; i < SystemConfig.getInstance().getBackendWorker(); i++) {
                backendExecutor.execute(new BackendCurrentRunnable(concurrentBackHandlerQueue));
            }

        } else {

            frontHandlerQueue = new LinkedBlockingDeque<>(SystemConfig.getInstance().getFrontWorker() * 3000);
            for (int i = 0; i < SystemConfig.getInstance().getFrontWorker(); i++) {
                frontExecutor.execute(new FrontendBlockRunnable((BlockingDeque<ServiceTask>) frontHandlerQueue));
            }

        }

        writeToBackendQueue = new LinkedBlockingQueue<>();
        for (int i = 0; i < SystemConfig.getInstance().getWriteToBackendWorker(); i++) {
            writeToBackendExecutor.execute(new WriteToBackendRunnable(writeToBackendQueue));
        }

        if (SystemConfig.getInstance().getUsingAIO() != 1) {
            frontRegisterQueue = new ConcurrentLinkedQueue<>();
            backendRegisterQueue = new ConcurrentLinkedQueue<>();
        }
    }

    private void initDbGroup() {
        Map<String, PhysicalDbGroup> dbGroups = this.getConfig().getDbGroups();
        LOGGER.info("Initialize dbGroup ...");
        for (PhysicalDbGroup node : dbGroups.values()) {
            node.init("OBsharding-D starts up");
        }
    }

    public void reloadSystemVariables(SystemVariables sys) {
        systemVariables = sys;
    }

    public IOProcessor nextFrontProcessor() {
        int i = ++nextFrontProcessor;
        if (i >= frontProcessors.length) {
            i = nextFrontProcessor = 0;
        }
        return frontProcessors[i];
    }

    public IOProcessor nextBackendProcessor() {
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


    public Deque<ServiceTask> getFrontHandlerQueue() {
        return frontHandlerQueue;
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
                            for (IOProcessor p : backendProcessors) {
                                p.checkBackendCons();
                            }
                        } catch (Exception e) {
                            LOGGER.info("checkBackendCons caught err:", e);
                        }
                    }
                });
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            for (IOProcessor p : frontProcessors) {
                                p.checkFrontCons();
                            }
                        } catch (Exception e) {
                            LOGGER.info("checkFrontCons caught err:", e);
                        }
                    }
                });
            }
        };
    }


    private void reviseSchemas() {
        if (systemVariables.isLowerCaseTableNames()) {
            config.reviseLowerCase();
            config.selfChecking0();
            config.loadSequence(OBsharding_DTempConfig.getInstance().getSequenceConfig());
            ConfigUtil.setSchemasForPool(config.getDbGroups(), config.getShardingNodes());
        } else {
            config.selfChecking0();
            config.loadSequence(OBsharding_DTempConfig.getInstance().getSequenceConfig());
        }
    }

    private void initOnlineStatus() throws Exception {
        if (ClusterConfig.getInstance().isClusterEnable()) {
            OnlineStatus.getInstance().mainThreadInitClusterOnline();
        }
    }

    // only for enable
    public void pullVarAndMeta(PhysicalDbGroup group) {
        if (config.isFullyConfigured()) {
            return;
        }
        LOGGER.info("begin to get variables Data");
        config.fulllyConfigured();
        group.getWriteDbInstance().setTestConnSuccess(true);
        HashMap<String, PhysicalDbGroup> groupMap = new HashMap<>(4);
        groupMap.put(group.getGroupName(), group);
        VarsExtractorHandler handler = new VarsExtractorHandler(groupMap);
        SystemVariables newSystemVariables = handler.execute();
        if (newSystemVariables == null) {
            // ignore, recover by reload
        } else {
            systemVariables = newSystemVariables;
        }
        LOGGER.info("end to get variables Data");
    }

    private void pullVarAndMeta() throws IOException {
        ProxyMetaManager tmManager = new ProxyMetaManager();
        ProxyMeta.getInstance().setTmManager(tmManager);
        if (this.getConfig().isFullyConfigured()) {
            LOGGER.info("begin to get variables Data");
            //init for sys VAR
            VarsExtractorHandler handler = new VarsExtractorHandler(config.getDbGroups());
            SystemVariables newSystemVariables = handler.execute();
            if (newSystemVariables == null) {
                throw new IOException("Can't get variables from all dbGroups");
            } else {
                systemVariables = newSystemVariables;
            }
            reviseSchemas();
            initDbGroup();
            LOGGER.info("end to get variables Data");
        } else {
            LOGGER.info("skip getting variables Data");
            reviseSchemas();
        }
        //init tmManager
        try {
            // zk first start will lazy init
            if (!ClusterConfig.getInstance().isInitZkFirst()) {
                tmManager.init(this.getConfig());
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
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
        id.append(SystemConfig.getInstance().getInstanceName());
        id.append(".");
        id.append(seq);
        id.append("'");
        return id.toString();
    }

    public void genXidSeq(String xaID) {
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

    public IOProcessor[] getFrontProcessors() {
        return frontProcessors;
    }

    public IOProcessor[] getBackendProcessors() {
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

    public GeneralLogProcessor getGeneralLogProcessor() {
        return generalLogProcessor;
    }

    public ExecutorService getFrontExecutor() {
        return frontExecutor;
    }

    public ExecutorService getWriteToBackendExecutor() {
        return writeToBackendExecutor;
    }

    public ExecutorService getBackendExecutor() {
        return backendExecutor;
    }

    public ExecutorService getNioFrontExecutor() {
        return nioFrontExecutor;
    }

    public ExecutorService getNioBackendExecutor() {
        return nioBackendExecutor;
    }

    public ConcurrentLinkedQueue<AbstractConnection> getFrontRegisterQueue() {
        return frontRegisterQueue;
    }

    public ConcurrentLinkedQueue<AbstractConnection> getBackendRegisterQueue() {
        return backendRegisterQueue;
    }

    public boolean isStartup() {
        return startup;
    }


    public Queue<ServiceTask> getConcurrentBackHandlerQueue() {
        return concurrentBackHandlerQueue;
    }

    public Map<String, Map<Thread, Runnable>> getRunnableMap() {
        return runnableMap;
    }

    public void setConcurrentBackHandlerQueue(Queue<ServiceTask> concurrentBackHandlerQueue) {
        this.concurrentBackHandlerQueue = concurrentBackHandlerQueue;
    }

    public long getXaIDInc() {
        return xaIDInc.get();
    }

    public void setConfig(ServerConfig config) {
        this.config = config;
    }
}
