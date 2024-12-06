/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.nio.handler.transaction.xa;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.alarm.AlarmCode;
import com.actiontech.dble.alarm.Alert;
import com.actiontech.dble.alarm.AlertUtil;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.config.ProblemReporter;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.config.util.StartProblemReporter;
import com.actiontech.dble.net.IOProcessor;
import com.actiontech.dble.net.connection.FrontendConnection;
import com.actiontech.dble.server.NonBlockingSession;
import com.actiontech.dble.services.mysqlsharding.ShardingService;
import com.actiontech.dble.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class XAAnalysisHandler extends XAHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(XAAnalysisHandler.class);
    private final ProblemReporter problemReporter = StartProblemReporter.getInstance();
    public static final Pattern XAID_STMT = Pattern.compile(DbleServer.NAME + "Server." + SystemConfig.getInstance().getInstanceName() + "[.](\\d+)(.[^\\s]+)?", Pattern.CASE_INSENSITIVE);

    public XAAnalysisHandler() {
        super();
    }

    public XAAnalysisHandler(PhysicalDbInstance pd) {
        super(pd);
    }

    public Map<PhysicalDbInstance, List<Map<String, String>>> select() {
        checkXA();
        return getXAResults();
    }

    public void checkResidualTask() {
        checkResidualXid(false);
    }

    public void checkResidualByStartup() {
        checkResidualXid(true);
    }

    public boolean isExistXid(String xaId) {
        boolean isExist = false;
        if (null != xaId) {
            Map<PhysicalDbInstance, List<Map<String, String>>> result = select();
            if (!result.isEmpty()) {
                List<Map<String, String>> values = result.get(result.keySet().stream().findFirst().get());
                if (values != null) {
                    String data = xaId.replace("'", "");
                    isExist = values.
                            stream().
                            anyMatch(x -> x.get("data").equalsIgnoreCase(data));
                }
            }
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("check xid is " + xaId + " is {} exist", isExist ? "" : "not");
        }
        return isExist;
    }

    private void checkResidualXid(boolean isStartup) {
        if (SystemConfig.getInstance().getBackendMode() == SystemConfig.BackendMode.OB) {
            return;
        }
        Set<Long> usedXaid = getCurrentUsedXaids();
        usedXaid.add(DbleServer.getInstance().getXaIDInc());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("The serial number of the xid being used:[{}]", usedXaid.stream().map(Objects::toString).collect(Collectors.joining(",")));
        }
        Long min = Collections.min(usedXaid);
        Map<PhysicalDbInstance, List<Map<String, String>>> result = select();
        String xaId;
        Matcher matcher;
        for (Map.Entry<PhysicalDbInstance, List<Map<String, String>>> rm : result.entrySet()) {
            if (result.get(rm.getKey()) == null)
                continue;
            boolean isResidual = false;
            List<String> residualXid = new ArrayList<>();
            for (Map<String, String> recover : result.get(rm.getKey())) {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Find xid: {} in {}.", recover.get("data"), rm.getKey());
                }
                matcher = XAID_STMT.matcher((xaId = recover.get("data")));
                // startup: when conforming to regular expressions, will be considered suspected xaId
                // timing task: smaller than the xaId in use, will be considered suspected xaId
                if (matcher.matches()) {
                    if (isStartup) {
                        isResidual = true;
                        residualXid.add(xaId);
                    } else if (Long.parseLong(matcher.group(1)) < min) {
                        isResidual = true;
                        residualXid.add(xaId);
                    }
                }
            }
            if (isResidual) {
                StringBuilder msg = new StringBuilder("Suspected residual xa transaction, ");
                msg.append("in the ");
                msg.append(rm.getKey().getConfig());
                msg.append(", have: [");
                msg.append(String.join(",", residualXid));
                msg.append("]. Please clean up according to the actual situation.");
                if (isStartup) {
                    // termination
                    problemReporter.error(msg.toString());
                } else {
                    LOGGER.warn(msg.toString());
                    Map<String, String> labels = AlertUtil.genSingleLabel("dbInstance", rm.getKey().getDbGroupConfig().getName() + "-" + rm.getKey().getConfig().getInstanceName());
                    AlertUtil.alertSelf(AlarmCode.XA_SUSPECTED_RESIDUE, Alert.AlertLevel.WARN, msg.toString(), labels);
                }
            }
        }
    }

    private Set<Long> getCurrentUsedXaids() {
        Set<Long> usedXaid = new HashSet<>();
        for (IOProcessor process : DbleServer.getInstance().getFrontProcessors()) {
            for (FrontendConnection front : process.getFrontends().values()) {
                if (!front.isManager() && front.getService() instanceof ShardingService) {
                    NonBlockingSession session = ((ShardingService) front.getService()).getSession2();
                    if (null != session.getTransactionManager().getSessionXaID()) {
                        Matcher matcher = XAID_STMT.matcher(StringUtil.removeApostropheOrBackQuote(session.getTransactionManager().getSessionXaID()));
                        if (matcher.matches()) {
                            usedXaid.add(Long.valueOf(matcher.group(1)));
                        }
                    }
                }
            }
        }
        return usedXaid;
    }
}
