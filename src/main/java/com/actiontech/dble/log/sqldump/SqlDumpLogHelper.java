package com.actiontech.dble.log.sqldump;

import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.net.mysql.MySQLPacket;
import com.actiontech.dble.route.parser.util.ParseUtil;
import com.actiontech.dble.rwsplit.RWSplitNonBlockingSession;
import com.actiontech.dble.server.parser.RwSplitServerParse;
import com.actiontech.dble.server.parser.ServerParseFactory;
import com.actiontech.dble.server.status.SqlDumpLog;
import com.actiontech.dble.services.mysqlsharding.MySQLResponseService;
import com.actiontech.dble.services.rwsplit.RWSplitService;
import com.actiontech.dble.util.StringUtil;
import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.visitor.ParameterizedOutputVisitorUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.*;
import org.apache.logging.log4j.core.appender.rolling.action.*;
import org.apache.logging.log4j.core.config.AppenderRef;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class SqlDumpLogHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlDumpLogHelper.class);
    private static final SqlDumpLogHelper INSTANCE = new SqlDumpLogHelper();

    private volatile boolean isOpen = false;
    private volatile ExtendedLogger logger;
    private static final ReentrantReadWriteLock LOCK = new ReentrantReadWriteLock();
    private static final RwSplitServerParse PARSER = ServerParseFactory.getRwSplitParser();

    private SqlDumpLogHelper() {
    }

    public static void init() {
        boolean isOpen0 = SqlDumpLog.getInstance().getEnableSqlDumpLog() == 1;
        String errMsg = onOff(isOpen0);
        if (isOpen0) {
            if (StringUtil.isEmpty(errMsg)) {
                LOGGER.info("SqlDumpLog's param: enableSqlDumpLog[{}], sqlDumpLogBasePath[{}], sqlDumpLogFileName[{}]," +
                                " sqlDumpLogCompressFilePattern[{}], sqlDumpLogCompressFilePath[{}], sqlDumpLogOnStartupRotate[{}]," +
                                " sqlDumpLogSizeBasedRotate[{}], sqlDumpLogTimeBasedRotate[{}], sqlDumpLogDeleteFileAge[{}]",
                        SqlDumpLog.getInstance().getEnableSqlDumpLog(), SqlDumpLog.getInstance().getSqlDumpLogBasePath(),
                        SqlDumpLog.getInstance().getSqlDumpLogFileName(), SqlDumpLog.getInstance().getSqlDumpLogCompressFilePattern(),
                        SqlDumpLog.getInstance().getSqlDumpLogCompressFilePath(), SqlDumpLog.getInstance().getSqlDumpLogOnStartupRotate(),
                        SqlDumpLog.getInstance().getSqlDumpLogSizeBasedRotate(), SqlDumpLog.getInstance().getSqlDumpLogTimeBasedRotate(),
                        SqlDumpLog.getInstance().getSqlDumpLogDeleteFileAge());
                LOGGER.info("===========================================Init SqlDumpLog Success=================================");
            } else {
                LOGGER.info("===========================================Init SqlDumpLog Fail=================================");
            }
        }
    }

    public static String onOff(boolean isOpen0) {
        final ReentrantReadWriteLock lock = INSTANCE.LOCK;
        lock.writeLock().lock();
        try {
            if (isOpen0 == INSTANCE.isOpen)
                return null;
            if (isOpen0) {
                try {
                    INSTANCE.isOpen = true;
                    SqlDumpLog.getInstance().setEnableSqlDumpLog(1);
                    INSTANCE.logger = SqlDumpLoggerUtil.getLogger(
                            SqlDumpLog.getInstance().getSqlDumpLogBasePath(), SqlDumpLog.getInstance().getSqlDumpLogFileName(),
                            SqlDumpLog.getInstance().getSqlDumpLogCompressFilePattern(),
                            SqlDumpLog.getInstance().getSqlDumpLogOnStartupRotate(), SqlDumpLog.getInstance().getSqlDumpLogSizeBasedRotate(), SqlDumpLog.getInstance().getSqlDumpLogTimeBasedRotate(),
                            SqlDumpLog.getInstance().getSqlDumpLogDeleteFileAge(), SqlDumpLog.getInstance().getSqlDumpLogCompressFilePath());
                } catch (Exception ei) { // rollback
                    try {
                        INSTANCE.isOpen = false;
                        SqlDumpLog.getInstance().setEnableSqlDumpLog(0);
                        INSTANCE.logger = null;
                        SqlDumpLoggerUtil.clearLogger();
                    } catch (Exception eii) {
                        if (LOGGER.isDebugEnabled())
                            LOGGER.debug("enable sqlDumpLog failed, rollback exception: {}", eii);
                        return "enable sqlDumpLog failed exception: " + ei.getMessage() + ", and rollback exception" + eii.getMessage();
                    }
                    if (LOGGER.isDebugEnabled())
                        LOGGER.debug("enable sqlDumpLog failed exception: {}", ei);
                    return "enable sqlDumpLog failed exception: " + ei.getMessage();
                }
            } else {
                try {
                    SqlDumpLoggerUtil.clearLogger();
                    INSTANCE.isOpen = false;
                    SqlDumpLog.getInstance().setEnableSqlDumpLog(0);
                    INSTANCE.logger = null;
                } catch (Exception ei) {
                    if (LOGGER.isDebugEnabled())
                        LOGGER.debug("disable sqlDumpLog failed exception: {}", ei);
                    return "disable sqlDumpLog failed exception: " + ei.getMessage();
                }
            }
            return null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public static void info(byte[] originPacket, boolean isHint, RWSplitService rwSplitService, MySQLResponseService responseService, long affectRows) {
        String[] arr = null;
        if (originPacket != null) {
            arr = packageLog(rwSplitService, originPacket, rwSplitService.getCharset().getResults());
        } else if (isHint) {
            arr = packageLog(rwSplitService.getSession2(), rwSplitService.getExecuteSql());
        } else {
            arr = packageLog(rwSplitService, rwSplitService.getExecuteSqlBytes(), rwSplitService.getCharset().getResults());
        }
        if (arr == null)
            return;

        // flush
        String sqlDigest;
        if (arr[1].equalsIgnoreCase("begin")) {
            sqlDigest = "begin";
        } else {
            sqlDigest = ParameterizedOutputVisitorUtils.parameterize(arr[1], DbType.mysql).replaceAll("[\\t\\n\\r]", " ");
        }
        String digestHash = Integer.toHexString(sqlDigest.hashCode()); // hashcode convert hex
        long dura = responseService.getConnection().getLastReadTime() - responseService.getConnection().getLastWriteTime();
        info0(digestHash, arr[0], rwSplitService.getTransactionsCounter() + "", affectRows, rwSplitService.getUser().getFullName(),
                rwSplitService.getConnection().getHost(), rwSplitService.getConnection().getLocalPort(),
                responseService.getConnection().getHost(), responseService.getConnection().getPort(), dura, sqlDigest);
    }


    private static void info0(String digestHash, String sqlType, String transactionId, long affectRows, String userName,
                              String clientHost, int clientPort,
                              String backHost, int backPort, long dura, String sqlDigest) {
        try {
            final ReentrantReadWriteLock lock = INSTANCE.LOCK;
            lock.readLock().lock();
            try {
                final ExtendedLogger log = INSTANCE.logger;
                if (log != null) {
                    sqlDigest = sqlDigest.length() > 100 ? sqlDigest.substring(0, 100) : sqlDigest;
                    log.info("[{}][{}][{}][{}][{}][{}:{}][{}:{}][{}] {}",
                            digestHash, sqlType, transactionId, affectRows, userName,
                            clientHost, clientPort, backHost, backPort, dura, sqlDigest);
                }
            } finally {
                lock.readLock().unlock();
            }
        } catch (Exception e) {
            LOGGER.warn("SqlDumpLog.log() happen exception: {}", e.getMessage());
        }
    }

    private static String[] packageLog(RWSplitService rwSplitService, byte[] data, String charset) {
        try {
            switch (data[4]) {
                case MySQLPacket.COM_QUERY:
                case MySQLPacket.COM_STMT_PREPARE:
                    MySQLMessage mm = new MySQLMessage(data);
                    mm.position(5);
                    String originSql = mm.readString(charset);
                    return packageLog(rwSplitService.getSession2(), originSql);
                default:
                    return null;
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return null;
    }

    private static String[] packageLog(RWSplitNonBlockingSession session2, String originSql) {
        String sql = originSql;
        if (session2.getRemingSql() != null)
            sql = session2.getRemingSql();

        int index = ParseUtil.findNextBreak(sql);
        boolean isMultiStatement = index + 1 < sql.length() && !ParseUtil.isEOF(sql, index);
        if (isMultiStatement) {
            session2.setRemingSql(sql.substring(index + 1));
            sql = sql.substring(0, ParseUtil.findNextBreak(sql));
        } else {
            session2.setRemingSql(null);
            if (sql.endsWith(";"))
                sql = sql.substring(0, sql.length() - 1);
        }
        return packageLog(sql.trim());
    }

    private static String[] packageLog(String originSql) {
        String[] arr = new String[2];
        int rs = PARSER.parse(originSql);
        int sqlType = rs & 0xff;
        switch (sqlType) {
            case RwSplitServerParse.SELECT:
                arr[0] = "SELECT";
                break;
            case RwSplitServerParse.INSERT:
                arr[0] = "INSERT";
                break;
            case RwSplitServerParse.DELETE:
                arr[0] = "DELETE";
                break;
            case RwSplitServerParse.UPDATE:
                arr[0] = "UPDATE";
                break;
            case RwSplitServerParse.DDL:
                arr[0] = "DDL";
                break;
            default:
                arr[0] = "OTHER";
                break;
        }
        arr[1] = originSql;
        return arr;
    }

    static class SqlDumpLoggerUtil {
        static final String LOG_NAME = "SqlDumpLog";
        static final String LOG_PATTERN = "[%d{yyyy-MM-dd HH:mm:ss.SSS}]%m%n";
        static final String ROLLOVER_MAX = "100";

        public static ExtendedLogger getLogger(String basePath, String fileName,
                                               String compressFilePattern,
                                               int onStartupRotate, String sizeBasedRotate, int timeBasedRotate,
                                               String deleteFileAge, String compressFilePath) throws Exception {

            final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
            final Configuration config = ctx.getConfiguration();
            /**
             * <PatternLayout>
             *    <Pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} %5p [%t] (%l) - %m%n</Pattern>
             * </PatternLayout>
             */
            final PatternLayout layout = PatternLayout.newBuilder().withPattern(LOG_PATTERN).withConfiguration(config).build();

            /**
             * <Policies>
             *    <OnStartupTriggeringPolicy/>
             *    <SizeBasedTriggeringPolicy size="250 MB"/>
             *    <TimeBasedTriggeringPolicy/>
             * </Policies>
             */
            List<TriggeringPolicy> policies = new ArrayList<>();
            if (participate(onStartupRotate)) {
                OnStartupTriggeringPolicy onStartupTriggeringPolicy = OnStartupTriggeringPolicy.createPolicy(1);
                policies.add(onStartupTriggeringPolicy);
            }
            if (participate(sizeBasedRotate)) {
                SizeBasedTriggeringPolicy sizeBasedTriggeringPolicy = SizeBasedTriggeringPolicy.createPolicy(sizeBasedRotate);
                policies.add(sizeBasedTriggeringPolicy);
            }
            if (participate(timeBasedRotate)) {
                TimeBasedTriggeringPolicy timeBasedTriggeringPolicy = TimeBasedTriggeringPolicy.createPolicy(timeBasedRotate + "", "false");
                policies.add(timeBasedTriggeringPolicy);
            }
            CompositeTriggeringPolicy triggeringPolicy = CompositeTriggeringPolicy.createPolicy(policies.toArray(new TriggeringPolicy[policies.size()]));

            //  <DefaultRolloverStrategy max="100">
            //     <Delete basePath="sqldump" maxDepth="5">
            //         <IfFileName glob="*/sqldump-*.log.gz">
            //               <IfLastModified age="2d"/>
            //         </IfFileName>
            //     </Delete>
            //  </DefaultRolloverStrategy>
            DeleteAction deleteAction = null;
            if (participate(deleteFileAge)) {
                IfLastModified ifLastModified = IfLastModified.createAgeCondition(Duration.parse(deleteFileAge), new PathCondition[0]);
                IfFileName ifFileName = IfFileName.createNameCondition(compressFilePath, null, new PathCondition[]{ifLastModified});
                deleteAction = DeleteAction.createDeleteAction(basePath, false, 5, false, null, new PathCondition[]{ifFileName}, null, config);
            }
            DefaultRolloverStrategy strategy = DefaultRolloverStrategy.newBuilder().withMax(ROLLOVER_MAX).withConfig(config).withCustomActions(new Action[]{deleteAction}).build();

            /**
             * <RollingFile name="SqlDumpLog" fileName="sqldump/sqldump.log"
             *              filePattern="sqldump/$${date:yyyy-MM}/sqldump-%d{MM-dd}-%i.log.gz">
             *   ...
             * </RollingFile>
             */
            RollingFileAppender appender = RollingFileAppender.newBuilder()
                    .withName(LOG_NAME)
                    .withFileName(basePath + File.separator + fileName)
                    .withFilePattern(basePath + File.separator + compressFilePattern)
                    .withLayout(layout)
                    .withPolicy(triggeringPolicy)
                    .withStrategy(strategy)
                    .withAppend(true)
                    .withBufferedIo(true)
                    .withImmediateFlush(true)
                    .withConfiguration(config)
                    .build();
            appender.start();
            config.addAppender(appender);

            /**
             * <AsyncLogger name="SqlDumpLog" additivity="false" includeLocation="false">
             *     <AppenderRef ref="SqlDumpLog"/>
             * </AsyncLogger>
             */
            LoggerConfig loggerConfig = LoggerConfig.createLogger("false", null, LOG_NAME, "false",
                    new AppenderRef[]{AppenderRef.createAppenderRef(LOG_NAME, null, null)},
                    null, config, null);
            loggerConfig.addAppender(appender, null, null);
            config.addLogger(LOG_NAME, loggerConfig);

            // update config
            ctx.updateLoggers();
            ExtendedLogger logger = ctx.getLogger(LOG_NAME);
            return logger;
        }


        public static void clearLogger() {
            final LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
            final Configuration config = ctx.getConfiguration();
            config.getAppender(LOG_NAME).stop();
            Appender appender = config.getAppenders().remove(LOG_NAME);
            if (appender != null)
                appender.stop();
            config.removeLogger(LOG_NAME);
        }

        private static boolean participate(Object value) {
            if (value instanceof String) {
                if (value.equals("-1"))
                    return false;
            } else {
                if ((int) value == -1)
                    return false;
            }
            return true;
        }
    }
}
