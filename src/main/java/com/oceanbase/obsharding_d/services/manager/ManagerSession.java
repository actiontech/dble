/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.manager;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.config.ErrorCode;
import com.oceanbase.obsharding_d.net.Session;
import com.oceanbase.obsharding_d.net.connection.BackendConnection;
import com.oceanbase.obsharding_d.net.connection.FrontendConnection;
import com.oceanbase.obsharding_d.plan.common.exception.MySQLOutPutException;
import com.oceanbase.obsharding_d.plan.node.PlanNode;
import com.oceanbase.obsharding_d.plan.optimizer.MyOptimizer;
import com.oceanbase.obsharding_d.plan.util.PlanUtil;
import com.oceanbase.obsharding_d.plan.visitor.MySQLPlanNodeVisitor;
import com.oceanbase.obsharding_d.services.manager.information.builder.ManagerHandlerBuilder;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLSyntaxErrorException;
import java.util.NoSuchElementException;

public class ManagerSession extends Session {
    public static final Logger LOGGER = LoggerFactory.getLogger(ManagerSession.class);
    private ManagerService managerService;


    public ManagerSession(ManagerService managerService) {
        this.managerService = managerService;
    }

    @Override
    public FrontendConnection getSource() {
        return managerService.getConnection();
    }

    @Override
    public void startFlowControl(int currentWritingSize) {
        //DO NOTHING
    }

    @Override
    public void stopFlowControl(int currentWritingSize) {
        //DO NOTHING
    }

    @Override
    public void releaseConnectionFromFlowControlled(BackendConnection con) {
        //DO NOTHING
    }

    public void execute(String schema, SQLSelectStatement statement) {
        MySQLPlanNodeVisitor visitor = new MySQLPlanNodeVisitor(schema, 45, null, false, null, null);
        visitor.visit(statement);
        PlanNode node = visitor.getTableNode();
        if (node.isCorrelatedSubQuery()) {
            throw new MySQLOutPutException(ErrorCode.ER_UNKNOWN_ERROR, "", "Correlated Sub Queries is not supported ");
        }
        node.setUpFields();
        node = MyOptimizer.managerOptimize(node);
        if (PlanUtil.containsSubQuery(node)) {
            final PlanNode finalNode = node;
            //sub Query build will be blocked, so use ComplexQueryExecutor
            OBsharding_DServer.getInstance().getComplexQueryExecutor().execute(() -> {
                executeMultiResultSet(finalNode);
            });
        } else {
            if (!visitor.isContainSchema()) {
                node.setAst(statement);
            }
            executeMultiResultSet(node);
        }
        int i = 0;

    }

    private void executeMultiResultSet(PlanNode node) {
        ManagerHandlerBuilder builder = new ManagerHandlerBuilder(node, this);
        try {
            builder.build();
        } catch (SQLSyntaxErrorException e) {
            LOGGER.info(managerService + " execute plan is : " + node, e);
            managerService.writeErrMessage(ErrorCode.ER_YES, "optimizer build error");
        } catch (NoSuchElementException e) {
            LOGGER.info(managerService + " execute plan is : " + node, e);
            managerService.writeErrMessage(ErrorCode.ER_NO_VALID_CONNECTION, "no valid connection");
        } catch (MySQLOutPutException e) {
            LOGGER.info(managerService + " execute plan is : " + node, e);
            managerService.writeErrMessage(e.getSqlState(), e.getMessage(), e.getErrorCode());
        } catch (Exception e) {
            LOGGER.info(managerService + " execute plan is : " + node, e);
            managerService.writeErrMessage(ErrorCode.ER_HANDLE_DATA, e.toString());
        }
    }
}
