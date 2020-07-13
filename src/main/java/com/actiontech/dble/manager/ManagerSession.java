/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.manager;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.manager.information.builder.ManagerHandlerBuilder;
import com.actiontech.dble.backend.mysql.nio.handler.query.impl.manager.ManagerOutputHandler;
import com.actiontech.dble.net.FrontendConnection;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.plan.common.exception.MySQLOutPutException;
import com.actiontech.dble.plan.node.PlanNode;
import com.actiontech.dble.plan.optimizer.MyOptimizer;
import com.actiontech.dble.plan.util.PlanUtil;
import com.actiontech.dble.plan.visitor.MySQLPlanNodeVisitor;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLSyntaxErrorException;
import java.util.NoSuchElementException;

public class ManagerSession extends Session {
    public static final Logger LOGGER = LoggerFactory.getLogger(ManagerSession.class);
    private ManagerConnection source;


    private ManagerOutputHandler outputHandler;
    public ManagerSession(ManagerConnection source) {
        this.source = source;
    }

    public ManagerOutputHandler getOutputHandler() {
        return outputHandler;
    }

    public void setOutputHandler(ManagerOutputHandler outputHandler) {
        this.outputHandler = outputHandler;
    }
    @Override
    public FrontendConnection getSource() {
        return source;
    }

    public void execute(String schema, SQLSelectStatement statement) {
        MySQLPlanNodeVisitor visitor = new MySQLPlanNodeVisitor(schema, 45, null, false, null);
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
            DbleServer.getInstance().getComplexQueryExecutor().execute(() -> {
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
        outputHandler = null;
        ManagerHandlerBuilder builder = new ManagerHandlerBuilder(node, this);
        try {
            builder.build();
        } catch (SQLSyntaxErrorException e) {
            LOGGER.info(source + " execute plan is : " + node, e);
            source.writeErrMessage(ErrorCode.ER_YES, "optimizer build error");
        } catch (NoSuchElementException e) {
            LOGGER.info(source + " execute plan is : " + node, e);
            source.writeErrMessage(ErrorCode.ER_NO_VALID_CONNECTION, "no valid connection");
        } catch (MySQLOutPutException e) {
            LOGGER.info(source + " execute plan is : " + node, e);
            source.writeErrMessage(e.getSqlState(), e.getMessage(), e.getErrorCode());
        } catch (Exception e) {
            LOGGER.info(source + " execute plan is : " + node, e);
            source.writeErrMessage(ErrorCode.ER_HANDLE_DATA, e.toString());
        }
    }
}
