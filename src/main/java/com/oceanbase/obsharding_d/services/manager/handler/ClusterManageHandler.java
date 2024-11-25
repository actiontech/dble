/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.services.manager.handler;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.cluster.AbstractGeneralListener;
import com.oceanbase.obsharding_d.cluster.ClusterGeneralConfig;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.config.model.ClusterConfig;
import com.oceanbase.obsharding_d.net.IOProcessor;
import com.oceanbase.obsharding_d.net.mysql.OkPacket;
import com.oceanbase.obsharding_d.net.service.AbstractService;
import com.oceanbase.obsharding_d.services.FrontendService;
import com.oceanbase.obsharding_d.services.manager.ManagerService;
import com.oceanbase.obsharding_d.util.DelayServiceControl;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author mycat
 */
public final class ClusterManageHandler {
    private static final Logger LOGGER = LogManager.getLogger(ClusterManageHandler.class);
    private static final Pattern PATTERN_DETACH = Pattern.compile("@@detach(\\s*timeout\\s*=\\s*([0-9]+))?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_ATTACH = Pattern.compile("@@attach(\\s*timeout\\s*=\\s*([0-9]+))?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final int DEFAULT_PAUSE_TIME = 10;
    private static volatile boolean detached = false;

    private ClusterManageHandler() {
    }


    public static synchronized void handle(String sql, ManagerService selfService, int offset) {

        String options = sql.substring(offset).trim();
        {
            Pattern pattern = PATTERN_DETACH;
            final Matcher matcher = pattern.matcher(options);
            if (matcher.matches()) {
                final String timeout = matcher.group(2);
                if (timeout == null) {
                    detach(DEFAULT_PAUSE_TIME, selfService);
                } else {
                    detach(Integer.parseInt(timeout), selfService);
                }

                return;
            }


        }
        {
            Pattern pattern = PATTERN_ATTACH;
            final Matcher matcher = pattern.matcher(options);
            if (matcher.matches()) {
                final String timeout = matcher.group(2);
                if (timeout == null) {
                    attach(DEFAULT_PAUSE_TIME, selfService);
                } else {
                    attach(Integer.parseInt(timeout), selfService);
                }
                return;
            }
        }
        selfService.writeErrMessage(ErrorCode.ER_YES, "Syntax incorrect");

    }

    public static boolean isDetached() {
        return detached;
    }

    private static synchronized void attach(int maxPauseTime, ManagerService selfService) {
        if (!ClusterConfig.getInstance().isClusterEnable()) {
            selfService.writeErrMessage(ErrorCode.ER_YES, "illegal state: attach cluster operation only can be used in cluster mode");
            return;
        }
        if (!detached) {
            selfService.writeErrMessage(ErrorCode.ER_YES, "illegal state: cluster is not detached");
            return;
        }


        //block other manager&ddl session send query
        ClusterGeneralConfig.getInstance().setNeedBlocked(true);
        try {
            //if the session is execute query already ,wait until the response return.
            LOGGER.info("cluster attach begin waiting");
            WaitResult waitResult = waitOtherSessionBlocked(selfService, maxPauseTime, false);
            LOGGER.info("cluster attach after waiting");
            if (!waitResult.success) {
                selfService.writeErrMessage(ErrorCode.ER_YES, "attach cluster pause timeout. " + waitResult.errorMessage);
                return;
            }
            ClusterGeneralConfig.getInstance().getClusterSender().attachCluster();
            detached = false;
            LOGGER.info("cluster attach complete");
        } catch (Exception e) {
            LOGGER.info("attach cluster err", e);
            selfService.writeErrMessage(ErrorCode.ER_YES, "attach cluster err：" + e);
            return;
        } finally {
            //resume task consume
            ClusterGeneralConfig.getInstance().setNeedBlocked(false);
            DelayServiceControl.getInstance().wakeUpAllBlockServices();
        }
        OkPacket okPacket = OkPacket.getDefault();
        okPacket.write(selfService.getConnection());

    }


    private static synchronized void detach(int maxPauseTime, ManagerService selfService) {
        if (!ClusterConfig.getInstance().isClusterEnable()) {
            selfService.writeErrMessage(ErrorCode.ER_YES, "illegal state: attach cluster operation only can be used in cluster mode");
            return;
        }
        if (detached) {
            selfService.writeErrMessage(ErrorCode.ER_YES, "illegal state: cluster is already detached");
            return;
        }


        //block other manager&ddl session send query
        ClusterGeneralConfig.getInstance().setNeedBlocked(true);
        try {
            //if the session is execute query already ,wait until the response return.
            LOGGER.info("cluster detach begin waiting");
            WaitResult waitResult = waitOtherSessionBlocked(selfService, maxPauseTime, true);
            LOGGER.info("cluster detach after waiting");
            if (!waitResult.success) {
                selfService.writeErrMessage(ErrorCode.ER_YES, "detach cluster pause timeout. " + waitResult.errorMessage);
                return;
            }

            ClusterGeneralConfig.getInstance().getClusterSender().detachCluster();
            detached = true;
            LOGGER.info("cluster detach complete");
        } catch (Exception e) {
            LOGGER.info("detach cluster err, try to resume", e);
            boolean resumeResult = false;
            try {
                ClusterGeneralConfig.getInstance().getClusterSender().attachCluster();
                LOGGER.info("detach resume success");
                resumeResult = true;
            } catch (Exception e2) {
                LOGGER.info("detach cluster resume err", e2);
            }
            selfService.writeErrMessage(ErrorCode.ER_YES, "detach cluster err：" + e + ", resume status:" + resumeResult);
            return;
        } finally {
            //resume task consume
            ClusterGeneralConfig.getInstance().setNeedBlocked(false);
            DelayServiceControl.getInstance().wakeUpAllBlockServices();
        }
        OkPacket okPacket = OkPacket.getDefault();
        okPacket.write(selfService.getConnection());


    }

    /**
     * return false is wait timeout
     *
     * @param maxPauseTime
     * @return
     */
    private static WaitResult waitOtherSessionBlocked(ManagerService selfService, int maxPauseTime, boolean isDetach) {
        List<FrontendService> waitServices = Lists.newArrayList();

        final long startTime = System.currentTimeMillis();
        /*
        if query is executing ,the isDoing must be true, then wait for complete.
        if the query is going to execute, it will be block,won't set Doing from false to true.
         */
        for (IOProcessor p : OBsharding_DServer.getInstance().getFrontProcessors()) {
            p.getFrontends().
                    values().
                    forEach(fc -> {
                        if (!fc.isAuthorized()) {
                            return;
                        }
                        final AbstractService frontService = fc.getService();
                        if (frontService == selfService) {
                            return;
                        }
                        if (frontService instanceof FrontendService) {
                            if (((FrontendService) frontService).getClusterDelayService().isDoing()) {
                                waitServices.add(((FrontendService) frontService));
                            }
                        }
                    });
        }

        while (System.currentTimeMillis() - startTime < maxPauseTime * 1000L) {

            waitServices.removeIf(tmpService -> !tmpService.getClusterDelayService().isDoing());
            if (waitServices.isEmpty()) {
                break;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
        if (!waitServices.isEmpty()) {
            final StringJoiner msg = new StringJoiner(",", "{", "}");
            waitServices.forEach(service -> {
                msg.add(String.valueOf(service.getConnection().getId()));
            });
            LOGGER.warn("cluster operation timeout because of {} connections, connections id is {}", waitServices.size(), msg);
            return WaitResult.ofError("some frontend connection is doing operation.");
        }
        if (!isDetach) {
            return WaitResult.ofSuccess();
        }

        /*
        detach should also guarantee ucore/zk listener isn't doing.
         */
        ClusterGeneralConfig.getInstance().getClusterSender().markDetach(true);
        while (System.currentTimeMillis() - startTime < maxPauseTime * 1000L && AbstractGeneralListener.getDoingCount().get() != 0) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }

        if (AbstractGeneralListener.getDoingCount().get() != 0) {
            LOGGER.warn("cluster operation timeout because of other server is doing cluster-related operation");
            ClusterGeneralConfig.getInstance().getClusterSender().markDetach(false);
            return WaitResult.ofError("some backend operation is doing. please check the OBsharding-D.log");
        }
        return WaitResult.ofSuccess();
    }


    static final class WaitResult {
        boolean success;
        String errorMessage;

        private WaitResult(String errorMessage) {
            this.success = false;
            this.errorMessage = errorMessage;
        }

        private WaitResult(boolean success) {
            this.success = success;
        }

        public static WaitResult ofSuccess() {
            return new WaitResult(true);
        }

        public static WaitResult ofError(String reason) {
            return new WaitResult(reason);
        }

    }
}
