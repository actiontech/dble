/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble;

import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.datasource.PhysicalDBNode;
import com.actiontech.dble.backend.datasource.PhysicalDBPool;
import com.actiontech.dble.backend.mysql.xa.*;
import com.actiontech.dble.backend.mysql.xa.recovery.Repository;
import com.actiontech.dble.backend.mysql.xa.recovery.impl.FileSystemRepository;
import com.actiontech.dble.backend.mysql.xa.recovery.impl.KVStoreRepository;
import com.actiontech.dble.buffer.BufferPool;
import com.actiontech.dble.buffer.DirectByteBufferPool;
import com.actiontech.dble.cache.CacheService;
import com.actiontech.dble.config.ServerConfig;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkConfig;
import com.actiontech.dble.config.loader.zkprocess.comm.ZkParamCfg;
import com.actiontech.dble.config.model.SchemaConfig;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.model.TableConfig;
import com.actiontech.dble.config.util.DnPropertyUtil;
import com.actiontech.dble.log.transaction.TxnLogProcessor;
import com.actiontech.dble.manager.ManagerConnectionFactory;
import com.actiontech.dble.memory.SeverMemory;
import com.actiontech.dble.memory.unsafe.Platform;
import com.actiontech.dble.meta.ProxyMetaManager;
import com.actiontech.dble.net.*;
import com.actiontech.dble.route.RouteService;
import com.actiontech.dble.route.sequence.handler.*;
import com.actiontech.dble.server.ServerConnectionFactory;
import com.actiontech.dble.server.util.GlobalTableUtil;
import com.actiontech.dble.sqlengine.OneRawSQLQueryResultHandler;
import com.actiontech.dble.sqlengine.SQLJob;
import com.actiontech.dble.statistic.stat.SqlResultSizeRecorder;
import com.actiontech.dble.statistic.stat.UserStat;
import com.actiontech.dble.statistic.stat.UserStatAnalyzer;
import com.actiontech.dble.util.*;
import com.google.common.io.Files;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.AsynchronousChannelGroup;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author mycat
 */
public final class DbleServer {

    public static final String NAME = "Dble_";
    private static final long TIME_UPDATE_PERIOD = 20L;
    private static final long DEFAULT_SQL_STAT_RECYCLE_PERIOD = 5 * 1000L;
    private static final long DEFAULT_OLD_CONNECTION_CLEAR_PERIOD = 5 * 1000L;

    private static final DbleServer INSTANCE = new DbleServer();
    private static final Logger LOGGER = LoggerFactory.getLogger("Server");
    private AtomicBoolean backupLocked;

    //global sequence
    private final SequenceHandler sequenceHandler;
    private final RouteService routerService;
    private final CacheService cacheService;
    private Properties dnIndexProperties;
    private ProxyMetaManager tmManager;
    private TxnLogProcessor txnLogProcessor;

    private AsynchronousChannelGroup[] asyncChannelGroups;
    private AtomicInteger channelIndex = new AtomicInteger();

    private volatile int nextProcessor;

    // System Buffer Pool Instance
    private BufferPool bufferPool;
    private boolean aio = false;

    private final AtomicLong xaIDInc = new AtomicLong();


    /**
     * Memory Manager
     */
    private SeverMemory serverMemory = null;

    private final ReentrantReadWriteLock confLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock metaLock = new ReentrantReadWriteLock();

    public static DbleServer getInstance() {
        return INSTANCE;
    }

    private final ServerConfig config;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean isOnline;
    private final long startupTime;
    private NIOProcessor[] processors;
    private SocketConnector connector;
    private NameableExecutor businessExecutor;
    private NameableExecutor complexQueryExecutor;
    private NameableExecutor timerExecutor;
    private InterProcessMutex dnindexLock;
    private long totalNetWorkBufferSize = 0;
    private XASessionCheck xaSessionCheck;

    private DbleServer() {
        this.config = new ServerConfig();
        scheduler = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("TimerScheduler-%d").build());

        /**
         * | offline | Change Server status to OFF |
         * | online | Change Server status to ON |
         */
        this.isOnline = new AtomicBoolean(true);

        //initialized the cache service
        cacheService = new CacheService(config.getSystem().isLowerCaseTableNames());

        //initialized the router cache and primary cache
        routerService = new RouteService(cacheService);

        // load data node active index from properties
        dnIndexProperties = DnPropertyUtil.loadDnIndexProps();

        this.startupTime = TimeUtil.currentTimeMillis();
        if (isUseZkSwitch()) {
            dnindexLock = new InterProcessMutex(ZKUtils.getConnection(), KVPathUtil.getDnIndexLockPath());
        }
        xaSessionCheck = new XASessionCheck();
        sequenceHandler = initSequenceHandler(config.getSystem().getSequnceHandlerType());
    }

    public SequenceHandler getSequenceHandler() {
        return sequenceHandler;
    }

    public long getTotalNetWorkBufferSize() {
        return totalNetWorkBufferSize;
    }

    public BufferPool getBufferPool() {
        return bufferPool;
    }

    public NameableExecutor getTimerExecutor() {
        return timerExecutor;
    }


    public NameableExecutor getComplexQueryExecutor() {
        return complexQueryExecutor;
    }

    public AtomicBoolean getBackupLocked() {
        return backupLocked;
    }

    public boolean isBackupLocked() {
        return backupLocked.get();
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
        if (isUseZK()) {
            id.append(ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_MYID));
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

    private SequenceHandler initSequenceHandler(int seqHandlerType) {
        switch (seqHandlerType) {
            case SystemConfig.SEQUENCE_HANDLER_MYSQL:
                return IncrSequenceMySQLHandler.getInstance();
            case SystemConfig.SEQUENCE_HANDLER_LOCAL_TIME:
                return IncrSequenceTimeHandler.getInstance();
            case SystemConfig.SEQUENCE_HANDLER_ZK_DISTRIBUTED:
                return DistributedSequenceHandler.getInstance();
            case SystemConfig.SEQUENCE_HANDLER_ZK_GLOBAL_INCREMENT:
                return IncrSequenceZKHandler.getInstance();
            default:
                throw new java.lang.IllegalArgumentException("Invalid sequnce handler type " + seqHandlerType);
        }
    }

    public SeverMemory getServerMemory() {
        return serverMemory;
    }

    public XASessionCheck getXaSessionCheck() {
        return xaSessionCheck;
    }

    /**
     * get next AsynchronousChannel ,first is exclude if multi
     * AsynchronousChannelGroups
     *
     * @return
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

    public ReentrantReadWriteLock getConfLock() {
        return confLock;
    }

    public ReentrantReadWriteLock getMetaLock() {
        return metaLock;
    }

    public ServerConfig getConfig() {
        confLock.readLock().lock();
        try {
            return config;
        } finally {
            confLock.readLock().unlock();
        }
    }

    public void beforeStart() {
        SystemConfig.getHomePath();
    }

    public void startup() throws IOException {
        SystemConfig system = config.getSystem();

        // server startup
        LOGGER.info("===============================================");
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

        backupLocked = new AtomicBoolean(false);
        // startup manager
        ManagerConnectionFactory mf = new ManagerConnectionFactory();
        ServerConnectionFactory sf = new ServerConnectionFactory();
        SocketAcceptor manager = null;
        SocketAcceptor server = null;
        aio = (system.getUsingAIO() == 1);

        // startup processors
        int processorCount = system.getProcessors();
        processors = new NIOProcessor[processorCount];
        // a page size
        int bufferPoolPageSize = system.getBufferPoolPageSize();
        // total page number
        short bufferPoolPageNumber = system.getBufferPoolPageNumber();
        //minimum allocation unit
        short bufferPoolChunkSize = system.getBufferPoolChunkSize();
        totalNetWorkBufferSize = bufferPoolPageSize * bufferPoolPageNumber;
        if (totalNetWorkBufferSize > Platform.getMaxDirectMemory()) {
            LOGGER.error("Direct BufferPool size lager than MaxDirectMemory");
            throw new IOException("Direct BufferPool size lager than MaxDirectMemory");
        }
        bufferPool = new DirectByteBufferPool(bufferPoolPageSize, bufferPoolChunkSize, bufferPoolPageNumber);


        /**
         * Off Heap For Merge/Order/Group/Limit
         */
        if (system.getUseOffHeapForMerge() == 1) {
            try {
                serverMemory = new SeverMemory(system, totalNetWorkBufferSize);
            } catch (NoSuchFieldException e) {
                LOGGER.error("NoSuchFieldException", e);
            } catch (IllegalAccessException e) {
                LOGGER.error("Error", e);
            }
        }
        int threadPoolSize = system.getProcessorExecutor();
        businessExecutor = ExecutorUtil.createFixed("BusinessExecutor", threadPoolSize);
        complexQueryExecutor = ExecutorUtil.createCached("complexQueryExecutor", threadPoolSize);
        timerExecutor = ExecutorUtil.createFixed("Timer", 1);

        for (int i = 0; i < processors.length; i++) {
            processors[i] = new NIOProcessor("Processor" + i, bufferPool, businessExecutor);
        }

        if (aio) {
            LOGGER.info("using aio network handler ");
            asyncChannelGroups = new AsynchronousChannelGroup[processorCount];
            // startup connector
            connector = new AIOConnector();
            for (int i = 0; i < processors.length; i++) {
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
            manager = new AIOAcceptor(NAME + "Manager", system.getBindIp(),
                    system.getManagerPort(), 100, mf, this.asyncChannelGroups[0]);

            // startup server

            server = new AIOAcceptor(NAME + "Server", system.getBindIp(),
                    system.getServerPort(), system.getServerBacklog(), sf, this.asyncChannelGroups[0]);

        } else {
            LOGGER.info("using nio network handler ");

            NIOReactorPool reactorPool = new NIOReactorPool(
                    DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + "NIOREACTOR",
                    processors.length);
            connector = new NIOConnector(DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + "NIOConnector", reactorPool);
            ((NIOConnector) connector).start();

            manager = new NIOAcceptor(DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + NAME + "Manager", system.getBindIp(),
                    system.getManagerPort(), 100, mf, reactorPool);

            server = new NIOAcceptor(DirectByteBufferPool.LOCAL_BUF_THREAD_PREX + NAME + "Server", system.getBindIp(),
                    system.getServerPort(), system.getServerBacklog(), sf, reactorPool);
        }

        // start transaction SQL log
        if (config.getSystem().getRecordTxn() == 1) {
            txnLogProcessor = new TxnLogProcessor(bufferPool);
            txnLogProcessor.setName("TxnLogProcessor");
            txnLogProcessor.start();
        }


        tmManager = new ProxyMetaManager();
        if (!this.getConfig().isDataHostWithoutWR()) {
            //init tmManager
            try {
                tmManager.init(this.getConfig());
            } catch (Exception e) {
                throw new IOException(e);
            }
        }


        //XA Init recovery Log
        LOGGER.info("===============================================");
        LOGGER.info("Perform XA recovery log ...");
        performXARecoveryLog();

        // manager start
        manager.start();
        LOGGER.info(manager.getName() + " is started and listening on " + manager.getPort());
        server.start();

        // server started
        LOGGER.info(server.getName() + " is started and listening on " + server.getPort());

        LOGGER.info("===============================================");


        long dataNodeIdleCheckPeriod = system.getDataNodeIdleCheckPeriod();
        scheduler.scheduleAtFixedRate(updateTime(), 0L, TIME_UPDATE_PERIOD, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(processorCheck(), 0L, system.getProcessorCheckPeriod(), TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(dataNodeConHeartBeatCheck(dataNodeIdleCheckPeriod), 0L, dataNodeIdleCheckPeriod, TimeUnit.MILLISECONDS);
        //dataHost heartBeat  will be influence by dataHostWithoutWR
        scheduler.scheduleAtFixedRate(dataNodeHeartbeat(), 0L, system.getDataNodeHeartbeatPeriod(), TimeUnit.MILLISECONDS);
        scheduler.scheduleAtFixedRate(dataSourceOldConsClear(), 0L, DEFAULT_OLD_CONNECTION_CLEAR_PERIOD, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(xaSessionCheck(), 0L, system.getXaSessionCheckPeriod(), TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(xaLogClean(), 0L, system.getXaLogCleanPeriod(), TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(resultSetMapClear(), 0L, system.getClearBigSqLResultSetMapMs(), TimeUnit.MILLISECONDS);
        if (system.getUseSqlStat() == 1) {
            //sql record detail timing clean
            scheduler.scheduleWithFixedDelay(recycleSqlStat(), 0L, DEFAULT_SQL_STAT_RECYCLE_PERIOD, TimeUnit.MILLISECONDS);
        }

        if (system.getUseGlobleTableCheck() == 1) {    // will be influence by dataHostWithoutWR
            scheduler.scheduleWithFixedDelay(globalTableConsistencyCheck(), 0L, system.getGlableTableCheckPeriod(), TimeUnit.MILLISECONDS);
        }


        if (!this.getConfig().isDataHostWithoutWR()) {
            // init datahost
            Map<String, PhysicalDBPool> dataHosts = this.getConfig().getDataHosts();
            LOGGER.info("Initialize dataHost ...");
            for (PhysicalDBPool node : dataHosts.values()) {
                String index = dnIndexProperties.getProperty(node.getHostName(), "0");
                if (!"0".equals(index)) {
                    LOGGER.info("init datahost: " + node.getHostName() + "  to use datasource index:" + index);
                }
                int activeIndex = node.init(Integer.parseInt(index));
                saveDataHostIndex(node.getHostName(), activeIndex);
                node.startHeartbeat();
            }
        }


        if (isUseZkSwitch()) {
            //upload the dnindex data to zk
            try {
                File file = new File(SystemConfig.getHomePath(), "conf" + File.separator + "dnindex.properties");
                dnindexLock.acquire(30, TimeUnit.SECONDS);
                String path = KVPathUtil.getDnIndexNode();
                CuratorFramework zk = ZKUtils.getConnection();
                if (zk.checkExists().forPath(path) == null) {
                    zk.create().creatingParentsIfNeeded().forPath(path, Files.toByteArray(file));
                }

            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    dnindexLock.release();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public void reloadMetaData(ServerConfig conf) {
        for (; ; ) {
            if (tmManager.getDdlCount() > 0) {
                continue;
            }
            metaLock.writeLock().lock();
            try {
                if (tmManager.getDdlCount() > 0) {
                    continue;
                }
                tmManager.terminate();
                tmManager = new ProxyMetaManager();
                tmManager.initMeta(conf);
                break;
            } finally {
                metaLock.writeLock().unlock();
            }
        }
    }

    public void reloadDnIndex() {
        if (DbleServer.getInstance().getProcessors() == null) return;
        // load datanode active index from properties
        dnIndexProperties = DnPropertyUtil.loadDnIndexProps();
        // init datahost
        Map<String, PhysicalDBPool> dataHosts = this.getConfig().getDataHosts();
        LOGGER.info("reInitialize dataHost ...");
        for (PhysicalDBPool node : dataHosts.values()) {
            String index = dnIndexProperties.getProperty(node.getHostName(), "0");
            if (!"0".equals(index)) {
                LOGGER.info("reinit datahost: " + node.getHostName() + "  to use datasource index:" + index);
            }
            node.switchSource(Integer.parseInt(index), true, "reload dnindex");

        }
    }


    /**
     * after reload @@config_all ,clean old connection
     *
     * @return
     */
    private Runnable dataSourceOldConsClear() {
        return new Runnable() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {

                        long sqlTimeout = DbleServer.getInstance().getConfig().getSystem().getSqlExecuteTimeout() * 1000L;
                        //close connection if now -lastTime>sqlExecuteTimeout
                        long currentTime = TimeUtil.currentTimeMillis();
                        Iterator<BackendConnection> iter = NIOProcessor.BACKENDS_OLD.iterator();
                        while (iter.hasNext()) {
                            BackendConnection con = iter.next();
                            long lastTime = con.getLastTime();
                            if (!con.isBorrowed() || currentTime - lastTime > sqlTimeout) {
                                con.close("clear old backend connection ...");
                                iter.remove();
                            }
                        }
                    }
                });
            }

        };
    }


    /**
     * clean up the data in UserStatAnalyzer
     */
    private Runnable resultSetMapClear() {
        return new Runnable() {
            @Override
            public void run() {
                try {
                    BufferPool pool = getBufferPool();
                    long bufferSize = pool.size();
                    long bufferCapacity = pool.capacity();
                    long bufferUsagePercent = (bufferCapacity - bufferSize) * 100 / bufferCapacity;
                    if (bufferUsagePercent < DbleServer.getInstance().getConfig().getSystem().getBufferUsagePercent()) {
                        Map<String, UserStat> map = UserStatAnalyzer.getInstance().getUserStatMap();
                        Set<String> userSet = DbleServer.getInstance().getConfig().getUsers().keySet();
                        for (String user : userSet) {
                            UserStat userStat = map.get(user);
                            if (userStat != null) {
                                SqlResultSizeRecorder recorder = userStat.getSqlResultSizeRecorder();
                                recorder.clearSqlResultSet();
                            }
                        }
                    }
                } catch (Exception e) {
                    LOGGER.warn("resultSetMapClear err " + e);
                }
            }

        };
    }

    /**
     * save cur datanode index to properties file
     *
     * @param
     * @param curIndex
     */
    public synchronized void saveDataHostIndex(String dataHost, int curIndex) {
        File file = new File(SystemConfig.getHomePath(), "conf" + File.separator + "dnindex.properties");
        FileOutputStream fileOut = null;
        try {
            String oldIndex = dnIndexProperties.getProperty(dataHost);
            String newIndex = String.valueOf(curIndex);
            if (newIndex.equals(oldIndex)) {
                return;
            }

            dnIndexProperties.setProperty(dataHost, newIndex);
            LOGGER.info("save DataHost index  " + dataHost + " cur index " + curIndex);

            File parent = file.getParentFile();
            if (parent != null && !parent.exists()) {
                if (!parent.mkdirs()) {
                    throw new IOException("mkdir " + parent.getAbsolutePath() + " error");
                }
            }

            fileOut = new FileOutputStream(file);
            dnIndexProperties.store(fileOut, "update");

            if (isUseZkSwitch()) {
                // save to  zk
                try {
                    dnindexLock.acquire(30, TimeUnit.SECONDS);
                    String path = KVPathUtil.getDnIndexNode();
                    CuratorFramework zk = ZKUtils.getConnection();
                    if (zk.checkExists().forPath(path) == null) {
                        zk.create().creatingParentsIfNeeded().forPath(path, Files.toByteArray(file));
                    } else {
                        byte[] data = zk.getData().forPath(path);
                        ByteArrayOutputStream out = new ByteArrayOutputStream();
                        Properties properties = new Properties();
                        properties.load(new ByteArrayInputStream(data));
                        if (!String.valueOf(curIndex).equals(properties.getProperty(dataHost))) {
                            properties.setProperty(dataHost, String.valueOf(curIndex));
                            properties.store(out, "update");
                            zk.setData().forPath(path, out.toByteArray());
                        }
                    }
                } finally {
                    dnindexLock.release();
                }
            }
        } catch (Exception e) {
            LOGGER.warn("saveDataNodeIndex err:", e);
        } finally {
            if (fileOut != null) {
                try {
                    fileOut.close();
                } catch (IOException e) {
                    //ignore error
                }
            }
        }
    }

    private boolean isUseZkSwitch() {
        return isUseZK() && this.config.getSystem().isUseZKSwitch();
    }

    public boolean isUseZK() {
        return Boolean.parseBoolean(ZkConfig.getInstance().getValue(ZkParamCfg.ZK_CFG_FLAG));
    }

    public TxnLogProcessor getTxnLogProcessor() {
        return txnLogProcessor;
    }

    public CacheService getCacheService() {
        return cacheService;
    }

    public NameableExecutor getBusinessExecutor() {
        return businessExecutor;
    }

    public RouteService getRouterService() {
        return routerService;
    }

    public NIOProcessor nextProcessor() {
        int i = ++nextProcessor;
        if (i >= processors.length) {
            i = nextProcessor = 0;
        }
        return processors[i];
    }

    public NIOProcessor[] getProcessors() {
        return processors;
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

    public ProxyMetaManager getTmManager() {
        metaLock.readLock().lock();
        try {
            return tmManager;
        } finally {
            metaLock.readLock().unlock();
        }
    }

    private Runnable updateTime() {
        return new Runnable() {
            @Override
            public void run() {
                TimeUtil.update();
            }
        };
    }

    // XA session check job
    private Runnable xaSessionCheck() {
        return new Runnable() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        xaSessionCheck.checkSessions();
                    }
                });
            }
        };
    }

    private Runnable xaLogClean() {
        return new Runnable() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        XAStateLog.cleanCompleteRecoverylog();
                    }
                });
            }
        };
    }

    // check the closed/overtime connection
    private Runnable processorCheck() {
        return new Runnable() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            for (NIOProcessor p : processors) {
                                p.checkBackendCons();
                            }
                        } catch (Exception e) {
                            LOGGER.warn("checkBackendCons caught err:" + e);
                        }
                    }
                });
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            for (NIOProcessor p : processors) {
                                p.checkFrontCons();
                            }
                        } catch (Exception e) {
                            LOGGER.warn("checkFrontCons caught err:" + e);
                        }
                    }
                });
            }
        };
    }

    // heartbeat for idle connection
    private Runnable dataNodeConHeartBeatCheck(final long heartPeriod) {
        return new Runnable() {
            @Override
            public void run() {
                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {

                        Map<String, PhysicalDBPool> nodes = DbleServer.getInstance().getConfig().getDataHosts();
                        for (PhysicalDBPool node : nodes.values()) {
                            node.heartbeatCheck(heartPeriod);
                        }

                        /*
                        Map<String, PhysicalDBPool> _nodes = MycatServer.getInstance().getConfig().getBackupDataHosts();
                        if (_nodes != null) {
                            for (PhysicalDBPool node : _nodes.values()) {
                                node.heartbeatCheck(heartPeriod);
                            }
                        }*/
                    }
                });
            }
        };
    }

    // heartbeat for data node
    private Runnable dataNodeHeartbeat() {
        return new Runnable() {
            @Override
            public void run() {

                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        if (!DbleServer.getInstance().getConfig().isDataHostWithoutWR()) {
                            Map<String, PhysicalDBPool> nodes = DbleServer.getInstance().getConfig().getDataHosts();
                            for (PhysicalDBPool node : nodes.values()) {
                                node.doHeartbeat();
                            }
                        }
                    }
                });
            }
        };
    }

    //clean up the old data in SqlStat
    private Runnable recycleSqlStat() {
        return new Runnable() {
            @Override
            public void run() {
                Map<String, UserStat> statMap = UserStatAnalyzer.getInstance().getUserStatMap();
                for (UserStat userStat : statMap.values()) {
                    userStat.getSqlLastStat().recycle();
                    userStat.getSqlRecorder().recycle();
                    userStat.getSqlHigh().recycle();
                    userStat.getSqlLargeRowStat().recycle();
                }
            }
        };
    }

    //  Table Consistency Check for global table
    private Runnable globalTableConsistencyCheck() {
        return new Runnable() {
            @Override
            public void run() {

                timerExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        if (!DbleServer.getInstance().getConfig().isDataHostWithoutWR()) {
                            GlobalTableUtil.consistencyCheck();
                        }
                    }
                });
            }
        };
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
                    coordinatorLogEntry.getTxState() == TxState.TX_COMMITING_STATE) {
                needCommit = true;
            } else if (coordinatorLogEntry.getTxState() == TxState.TX_ROLLBACK_FAILED_STATE ||
                    //don't konw prepare is successed or not ,should rollback
                    coordinatorLogEntry.getTxState() == TxState.TX_PREPARE_UNCONNECT_STATE ||
                    // will rollbacking, may send but failed receiving,should rollback agagin
                    coordinatorLogEntry.getTxState() == TxState.TX_ROLLBACKING_STATE ||
                    // will preparing, may send but failed receiving,should rollback agagin
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
        boolean finished = true;
        for (int j = 0; j < coordinatorLogEntry.getParticipants().length; j++) {
            ParticipantLogEntry participantLogEntry = coordinatorLogEntry.getParticipants()[j];
            // XA commit
            if (participantLogEntry.getTxState() != TxState.TX_COMMIT_FAILED_STATE &&
                    participantLogEntry.getTxState() != TxState.TX_COMMITING_STATE &&
                    participantLogEntry.getTxState() != TxState.TX_PREPARE_UNCONNECT_STATE &&
                    participantLogEntry.getTxState() != TxState.TX_ROLLBACKING_STATE &&
                    participantLogEntry.getTxState() != TxState.TX_ROLLBACK_FAILED_STATE &&
                    participantLogEntry.getTxState() != TxState.TX_PREPARED_STATE) {
                continue;
            }
            finished = false;
            outLoop:
            for (SchemaConfig schema : DbleServer.getInstance().getConfig().getSchemas().values()) {
                for (TableConfig table : schema.getTables().values()) {
                    for (String dataNode : table.getDataNodes()) {
                        PhysicalDBNode dn = DbleServer.getInstance().getConfig().getDataNodes().get(dataNode);
                        if (participantLogEntry.compareAddress(dn.getDbPool().getSource().getConfig().getIp(), dn.getDbPool().getSource().getConfig().getPort(), dn.getDatabase())) {
                            OneRawSQLQueryResultHandler resultHandler = new OneRawSQLQueryResultHandler(new String[0], new XARecoverCallback(needCommit, participantLogEntry));
                            xaCmd.append(coordinatorLogEntry.getId().substring(0, coordinatorLogEntry.getId().length() - 1));
                            xaCmd.append(".");
                            xaCmd.append(dn.getDatabase());
                            xaCmd.append("'");
                            SQLJob sqlJob = new SQLJob(xaCmd.toString(), dn.getDatabase(), resultHandler, dn.getDbPool().getSource());
                            sqlJob.run();
                            LOGGER.debug(String.format("[%s] Host:[%s] schema:[%s]", xaCmd, dn.getName(), dn.getDatabase()));
                            break outLoop;
                        }
                    }
                }
            }
        }
        if (finished) {
            XAStateLog.saveXARecoverylog(coordinatorLogEntry.getId(), needCommit ? TxState.TX_COMMITED_STATE : TxState.TX_ROLLBACKED_STATE);
            XAStateLog.writeCheckpoint(coordinatorLogEntry.getId());
        }
    }

    /**
     * covert the collection to array
     **/
    private CoordinatorLogEntry[] getCoordinatorLogEntries() {
        Repository fileRepository = isUseZK() ? new KVStoreRepository() : new FileSystemRepository();
        Collection<CoordinatorLogEntry> allCoordinatorLogEntries = fileRepository.getAllCoordinatorLogEntries();
        fileRepository.close();
        if (allCoordinatorLogEntries == null) {
            return new CoordinatorLogEntry[0];
        }
        if (allCoordinatorLogEntries.size() == 0) {
            return new CoordinatorLogEntry[0];
        }
        return allCoordinatorLogEntries.toArray(new CoordinatorLogEntry[allCoordinatorLogEntries.size()]);
    }


    public boolean isAIO() {
        return aio;
    }

}
