/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble;

import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.mysql.xa.XaCheckHandler;
import com.actiontech.dble.buffer.DirectByteBufferPool;
import com.actiontech.dble.cluster.zkprocess.zktoxml.ZktoXmlMain;
import com.actiontech.dble.config.DbleTempConfig;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.util.ConfigUtil;
import com.actiontech.dble.log.general.GeneralLogProcessor;
import com.actiontech.dble.log.transaction.TxnLogProcessor;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.SocketAcceptor;
import com.actiontech.dble.net.SocketConnector;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.executor.BackendCurrentRunnable;
import com.actiontech.dble.net.executor.FrontendBlockRunnable;
import com.actiontech.dble.net.executor.FrontendCurrentRunnable;
import com.actiontech.dble.net.executor.WriteToBackendRunnable;
import com.actiontech.dble.net.impl.aio.AIOAcceptor;
import com.actiontech.dble.net.impl.aio.AIOConnector;
import com.actiontech.dble.net.impl.nio.NIOAcceptor;
import com.actiontech.dble.net.impl.nio.NIOConnector;
import com.actiontech.dble.net.impl.nio.RW;
import com.actiontech.dble.net.mysql.WriteToBackendTask;
import com.actiontech.dble.net.service.ServiceTask;
import com.actiontech.dble.net.ssl.SSLWrapperRegistry;
import com.actiontech.dble.server.status.SlowQueryLog;
import com.actiontech.dble.server.variables.SystemVariables;
import com.actiontech.dble.server.variables.VarsExtractorHandler;
import com.actiontech.dble.services.factorys.ManagerConnectionFactory;
import com.actiontech.dble.services.factorys.ServerConnectionFactory;
import com.actiontech.dble.singleton.*;
import com.actiontech.dble.statistic.sql.StatisticManager;
import com.actiontech.dble.statistic.stat.ThreadWorkUsage;
import com.actiontech.dble.util.ExecutorUtil;
import com.actiontech.dble.util.TimeUtil;
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

public final class DbleServer {

    public static final String NAME = "Dble_";

    private static final DbleServer INSTANCE = new DbleServer();
    private static final Logger LOGGER = LoggerFactory.getLogger("Server");
    //used by manager command show @@binlog_status to get a stable GTID
    private final AtomicBoolean backupLocked = new AtomicBoolean(false);

    public static final String BUSINESS_EXECUTOR_NAME = "BusinessExecutor";
    public static final String BACKEND_BUSINESS_EXECUTOR_NAME = "backendBusinessExecutor";
    public static final String WRITE_TO_BACKEND_EXECUTOR_NAME = "writeToBackendExecutor";
    public static final String COMPLEX_QUERY_EXECUTOR_NAME = "complexQueryExecutor";
    public static final String TIMER_EXECUTOR_NAME = "Timer";
    public static final String FRONT_EXECUTOR_NAME = DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + "NIO_REACTOR_FRONT-";
    public static final String BACKEND_EXECUTOR_NAME = DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + "NIO_REACTOR_BACKEND-";
    public static final String FRONT_BACKEND_SUFFIX = "-RW";
    public static final String AIO_EXECUTOR_NAME = DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + "AIO";

    private volatile SystemVariables systemVariables = new SystemVariables();
    private TxnLogProcessor txnLogProcessor;
    private GeneralLogProcessor generalLogProcessor;

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
    private IOProcessor[] frontProcessors;
    private IOProcessor[] backendProcessors;
    private SocketConnector connector;
    private ExecutorService frontExecutor;
    private ExecutorService backendExecutor;
    private ExecutorService businessExecutor;
    private ExecutorService backendBusinessExecutor;
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

    private DbleServer() {
    }


    public void startup() throws Exception {
        LOGGER.info("===========================================DBLE SERVER STARTING===================================");
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
        int frontProcessorCount = SystemConfig.getInstance().getProcessors();
        int backendProcessorCount = SystemConfig.getInstance().getBackendProcessors();
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
                frontExecutor.execute(new RW(frontRegisterQueue));
            }
            for (int i = 0; i < backendProcessorCount; i++) {
                backendExecutor.execute(new RW(backendRegisterQueue));
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

        CronScheduler.getInstance().init(config.getSchemas());
        LOGGER.info("====================================CronScheduler started=========================================");

        CustomMySQLHa.getInstance().start();
        LOGGER.info("======================================ALL START INIT FINISH=======================================");
        startup = true;
    }

    private void initExecutor(int frontProcessorCount, int backendProcessorCount) {
        businessExecutor = ExecutorUtil.createFixed(BUSINESS_EXECUTOR_NAME, SystemConfig.getInstance().getProcessorExecutor(), runnableMap);
        backendBusinessExecutor = ExecutorUtil.createFixed(BACKEND_BUSINESS_EXECUTOR_NAME, SystemConfig.getInstance().getBackendProcessorExecutor(), runnableMap);
        writeToBackendExecutor = ExecutorUtil.createFixed(WRITE_TO_BACKEND_EXECUTOR_NAME, SystemConfig.getInstance().getWriteToBackendExecutor(), runnableMap);
        complexQueryExecutor = ExecutorUtil.createCached(COMPLEX_QUERY_EXECUTOR_NAME, SystemConfig.getInstance().getComplexExecutor(), null);
        timerExecutor = ExecutorUtil.createFixed(TIMER_EXECUTOR_NAME, 1);
        frontExecutor = ExecutorUtil.createFixed(FRONT_EXECUTOR_NAME, FRONT_BACKEND_SUFFIX, frontProcessorCount, runnableMap);
        backendExecutor = ExecutorUtil.createFixed(BACKEND_EXECUTOR_NAME, FRONT_BACKEND_SUFFIX, backendProcessorCount, runnableMap);
    }

    private void initServerConfig() throws Exception {
        //compatible with ZK first initialized
        if (ClusterConfig.getInstance().isClusterEnable() && !ClusterConfig.getInstance().isInitZkFirst()) {
            this.config = new ServerConfig(DbleTempConfig.getInstance().getUserConfig(), DbleTempConfig.getInstance().getDbConfig(),
                    DbleTempConfig.getInstance().getShardingConfig(), DbleTempConfig.getInstance().getSequenceConfig());
            DbleTempConfig.getInstance().clean();
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
            for (int i = 0; i < SystemConfig.getInstance().getProcessorExecutor(); i++) {
                businessExecutor.execute(new FrontendCurrentRunnable(frontHandlerQueue));
            }

            concurrentBackHandlerQueue = new ConcurrentLinkedQueue<>();
            for (int i = 0; i < SystemConfig.getInstance().getBackendProcessorExecutor(); i++) {
                backendBusinessExecutor.execute(new BackendCurrentRunnable(concurrentBackHandlerQueue));
            }

        } else {

            frontHandlerQueue = new LinkedBlockingDeque<>(SystemConfig.getInstance().getProcessorExecutor() * 3000);
            for (int i = 0; i < SystemConfig.getInstance().getProcessorExecutor(); i++) {
                businessExecutor.execute(new FrontendBlockRunnable((BlockingDeque<ServiceTask>) frontHandlerQueue));
            }

        }

        writeToBackendQueue = new LinkedBlockingQueue<>();
        for (int i = 0; i < SystemConfig.getInstance().getWriteToBackendExecutor(); i++) {
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
            node.init("dble starts up");
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
            config.loadSequence(DbleTempConfig.getInstance().getSequenceConfig());
            config.selfChecking0();
            ConfigUtil.setSchemasForPool(config.getDbGroups(), config.getShardingNodes());
        } else {
            config.loadSequence(DbleTempConfig.getInstance().getSequenceConfig());
            config.selfChecking0();
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

    public ExecutorService getBusinessExecutor() {
        return businessExecutor;
    }

    public ExecutorService getWriteToBackendExecutor() {
        return writeToBackendExecutor;
    }

    public ExecutorService getBackendBusinessExecutor() {
        return backendBusinessExecutor;
    }

    public ExecutorService getFrontExecutor() {
        return frontExecutor;
    }

    public ExecutorService getBackendExecutor() {
        return backendExecutor;
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
}
