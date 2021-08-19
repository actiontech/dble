/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.services.manager.handler;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.cluster.ClusterGeneralConfig;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.ClusterConfig;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.mysql.OkPacket;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.services.FrontendService;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.util.DelayServiceControl;
import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author mycat
 */
public final class ClusterManageHandler {
    private static final Logger LOGGER = LogManager.getLogger(ClusterManageHandler.class);
    private static final Pattern PATTERN_DETACH = Pattern.compile("^\\s*cluster\\s*@@detach(\\s*timeout\\s*=\\s*([0-9]+))?\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern PATTERN_ATTACH = Pattern.compile("^\\s*cluster\\s*@@attach(\\s*timeout\\s*=\\s*([0-9]+))?\\s*$", Pattern.CASE_INSENSITIVE);
    public static final int DEFAULT_PAUSE_TIME = 10;
    private static boolean detached = false;

    private ClusterManageHandler() {
    }


    public static synchronized void handle(String sql, ManagerService selfService) {
        {
            Pattern pattern = PATTERN_DETACH;
            final Matcher matcher = pattern.matcher(sql);
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
            final Matcher matcher = pattern.matcher(sql);
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
            boolean success = waitOtherSessionBlocked(selfService, maxPauseTime);
            LOGGER.info("cluster attach after waiting");
            if (!success) {
                selfService.writeErrMessage(ErrorCode.ER_YES, "attach cluster pause timeout");
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
            boolean success = waitOtherSessionBlocked(selfService, maxPauseTime);
            LOGGER.info("cluster detach after waiting");
            if (!success) {
                selfService.writeErrMessage(ErrorCode.ER_YES, "detach cluster pause timeout");
                return;
            }

            ClusterGeneralConfig.getInstance().getClusterSender().detachCluster();
            detached = true;
            LOGGER.info("cluster detach complete");
        } catch (Exception e) {
            LOGGER.info("detach cluster err, try to resume", e);
            try {
                ClusterGeneralConfig.getInstance().getClusterSender().attachCluster();
                LOGGER.info("detach resume success");
            } catch (Exception e2) {
                LOGGER.info("detach cluster resume err", e2);
            }
            selfService.writeErrMessage(ErrorCode.ER_YES, "detach cluster err：" + e);
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
    private static boolean waitOtherSessionBlocked(ManagerService selfService, int maxPauseTime) {
        List<FrontendService> waitServices = Lists.newArrayList();

        final long startTime = System.currentTimeMillis();
        for (IOProcessor p : DbleServer.getInstance().getFrontProcessors()) {
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
        if (waitServices.isEmpty()) {
            return true;
        }
        while (System.currentTimeMillis() - startTime < maxPauseTime * 1000L) {

            waitServices.removeIf(tmpService -> !tmpService.getClusterDelayService().isDoing());
            if (waitServices.isEmpty()) {
                return true;
            }
        }
        return false;
    }

}
