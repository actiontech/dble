/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.log.sqldump;

import com.oceanbase.obsharding_d.backend.mysql.ByteUtil;
import com.oceanbase.obsharding_d.net.mysql.MySQLPacket;
import com.oceanbase.obsharding_d.server.parser.ServerParseFactory;
import com.oceanbase.obsharding_d.server.parser.ShardingServerParse;
import com.oceanbase.obsharding_d.server.status.SqlDumpLog;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.services.rwsplit.RWSplitService;
import com.oceanbase.obsharding_d.services.rwsplit.handle.PreparedStatementHolder;
import com.oceanbase.obsharding_d.util.SqlStringUtil;
import com.oceanbase.obsharding_d.util.StringUtil;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class SqlDumpLogHelper {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlDumpLogHelper.class);
    private static final SqlDumpLogHelper INSTANCE = new SqlDumpLogHelper();

    private volatile boolean isOpen = false;
    private volatile ExtendedLogger logger;
    private static final ReentrantReadWriteLock LOCK = new ReentrantReadWriteLock();
    private static final ShardingServerParse PARSER = ServerParseFactory.getShardingParser(); // the uniform use of ShardingServerParse

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
                        LOGGER.warn("enable sqlDumpLog failed, rollback exception: {}", eii);
                        return "enable sqlDumpLog failed exception: " + ei.getMessage() + ", and rollback exception" + eii.getMessage();
                    }
                    LOGGER.warn("enable sqlDumpLog failed exception: {}", ei);
                    return "enable sqlDumpLog failed exception: " + ei.getMessage();
                }
            } else {
                try {
                    SqlDumpLoggerUtil.clearLogger();
                    INSTANCE.isOpen = false;
                    SqlDumpLog.getInstance().setEnableSqlDumpLog(0);
                    INSTANCE.logger = null;
                } catch (Exception ei) {
                    LOGGER.warn("disable sqlDumpLog failed exception: {}", ei);
                    return "disable sqlDumpLog failed exception: " + ei.getMessage();
                }
            }
            return null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public static void info(String sql, byte[] originPacket, RWSplitService rwSplitService, MySQLResponseService responseService, long affectRows) {
        try {
            if (SqlDumpLog.getInstance().getEnableSqlDumpLog() != 1)
                return;

            String[] arr = packageLog(originPacket, sql, rwSplitService);
            if (arr == null)
                return;

            String sqlDigest;
            int sqlDigestHash;
            if (arr[1].equalsIgnoreCase("begin")) {
                sqlDigest = "begin";
                sqlDigestHash = sqlDigest.hashCode();
            } else {
                try {
                    sqlDigest = ParameterizedOutputVisitorUtils.parameterize(arr[1], DbType.mysql).replaceAll("[\\t\\n\\r]", " ");
                    sqlDigestHash = sqlDigest.hashCode();
                } catch (RuntimeException ex) {
                    sqlDigestHash = arr[1].hashCode();
                    sqlDigest = "Other";
                }
            }
            String digestHash = Integer.toHexString(sqlDigestHash); // hashcode convert hex
            long dura = responseService.getConnection().getLastReadTime() - responseService.getConnection().getLastWriteTime();
            info0(digestHash, arr[0], rwSplitService.getTxId() + "", affectRows, rwSplitService.getUser().getFullName(),
                    rwSplitService.getConnection().getHost(), rwSplitService.getConnection().getLocalPort(),
                    responseService.getConnection().getHost(), responseService.getConnection().getPort(), dura, sqlDigest);
        } catch (Exception e) {
            LOGGER.warn("SqlDumpLogHelper.info() exception: {}", e);
        }
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
                    sqlDigest = sqlDigest.length() > 1024 ? sqlDigest.substring(0, 1024) : sqlDigest;
                    log.info("[{}][{}][{}][{}][{}][{}:{}][{}:{}][{}] {}",
                            digestHash, sqlType, transactionId, affectRows, userName,
                            clientHost, clientPort, backHost, backPort, dura, sqlDigest);
                }
            } finally {
                lock.readLock().unlock();
            }
        } catch (Exception e) {
            LOGGER.warn("SqlDumpLogHelper.info() happen exception: {}", e.getMessage());
        }
    }

    private static String[] packageLog(byte[] data, String sql, RWSplitService rwSplitService) {
        if (data != null) {
            if (data.length < 5) return null;
            switch (data[4]) {
                case MySQLPacket.COM_QUERY:
                    return packageLog(sql);
                case MySQLPacket.COM_STMT_EXECUTE:
                    long statementId = ByteUtil.readUB4(data, 5);
                    PreparedStatementHolder holder = rwSplitService.getPrepareStatement(statementId);
                    return packageLog(holder.getPrepareSql());
                default:
                    return null;
            }
        } else if (sql != null) {
            return packageLog(sql);
        }
        return null;
    }

    private static String[] packageLog(String originSql) {
        if (originSql == null)
            return null;
        String[] arr = new String[2];
        int rs = PARSER.parse(originSql);
        int sqlType = rs & 0xff;
        arr[0] = SqlStringUtil.getSqlType(sqlType);
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

            SizeBasedTriggeringPolicy sizeBasedTriggeringPolicy = SizeBasedTriggeringPolicy.createPolicy(sizeBasedRotate);
            policies.add(sizeBasedTriggeringPolicy);

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
            IfLastModified ifLastModified = IfLastModified.createAgeCondition(Duration.parse(deleteFileAge), new PathCondition[0]);
            IfFileName ifFileName = IfFileName.createNameCondition(compressFilePath, null, new PathCondition[]{ifLastModified});
            DeleteAction deleteAction = DeleteAction.createDeleteAction(basePath, false, 5, false, null, new PathCondition[]{ifFileName}, null, config);
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
            if (config == null) return;
            Appender appender = config.getAppenders().remove(LOG_NAME);
            if (appender != null)
                appender.stop();
            config.removeLogger(LOG_NAME);
        }

        private static boolean participate(Object value) {
            if (value instanceof String) {
                String value0 = ((String) value).trim();
                if (StringUtil.isBlank(value0) || value.equals("-1") || value0.equalsIgnoreCase("null"))
                    return false;
            } else {
                if ((int) value == -1)
                    return false;
            }
            return true;
        }
    }
}
