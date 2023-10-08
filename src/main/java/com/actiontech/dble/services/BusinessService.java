/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services;

import com.actiontech.dble.backend.mysql.MySQLMessage;
import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.config.model.user.UserConfig;
import com.actiontech.dble.net.Session;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.EOFPacket;
import com.actiontech.dble.net.service.AuthResultInfo;
import com.actiontech.dble.server.variables.MysqlVariable;
import com.actiontech.dble.singleton.TsQueriesCounter;
import com.actiontech.dble.statistic.CommandCount;

import java.util.concurrent.atomic.AtomicLong;

public abstract class BusinessService<T extends UserConfig> extends TransactionService<T> {

    private final AtomicLong queriesCounter = new AtomicLong(0);

    private volatile boolean isLockTable;
    protected final CommandCount commands;

    public BusinessService(AbstractConnection connection, AuthResultInfo info) {
        super(connection, info);
        this.commands = connection.getProcessor().getCommands();
    }

    public boolean isLockTable() {
        return isLockTable;
    }

    public void setLockTable(boolean locked) {
        isLockTable = locked;
    }

    public void queryCount() {
        queriesCounter.incrementAndGet();
    }

    public long getQueriesCounter() {
        return queriesCounter.get();
    }

    public void resetCounter() {
        queriesCounter.set(Long.MIN_VALUE);
    }

    public void addHisQueriesCount() {
        TsQueriesCounter.getInstance().addToHistory(this);
    }

    public void executeContextSetTask(MysqlVariable[] contextTask) {
        MysqlVariable autocommitItem = null;
        for (MysqlVariable variable : contextTask) {
            switch (variable.getType()) {
                case CHARACTER_SET_CLIENT:
                    String charsetClient = variable.getValue();
                    this.setCharacterClient(charsetClient);
                    break;
                case CHARACTER_SET_CONNECTION:
                    String collationName = variable.getValue();
                    this.setCharacterConnection(collationName);
                    break;
                case CHARACTER_SET_RESULTS:
                    String charsetResult = variable.getValue();
                    this.setCharacterResults(charsetResult);
                    break;
                case COLLATION_CONNECTION:
                    String collation = variable.getValue();
                    this.setCollationConnection(collation);
                    break;
                case TX_ISOLATION:
                    String isolationLevel = variable.getValue();
                    this.setTxIsolation(Integer.parseInt(isolationLevel));
                    break;
                case TX_READ_ONLY:
                    sessionReadOnly = Boolean.parseBoolean(variable.getValue());
                    break;
                case SYSTEM_VARIABLES:
                    this.sysVariables.put(variable.getName(), variable.getValue());
                    break;
                case USER_VARIABLES:
                    if (variable.getValue() != null) {
                        this.usrVariables.put(variable.getName(), variable.getValue());
                    }
                    break;
                case CHARSET:
                    this.setNames(variable.getValue(), "@@session.collation_database");
                    break;
                case NAMES:
                    String[] charsetAndCollate = variable.getValue().split(":");
                    this.setNames(charsetAndCollate[0], charsetAndCollate[1]);
                    break;
                case AUTOCOMMIT:
                    autocommitItem = variable;
                    break;
                default:
                    handleVariable(variable);
                    break;
            }
        }

        if (autocommitItem == null) {
            controlTx(TransactionOperate.QUERY);
            writeOkPacket();
        } else {
            handleVariable(autocommitItem);
        }
    }

    protected void setOption(byte[] data) {
        MySQLMessage mm = new MySQLMessage(data); //see sql\protocol_classic.cc parse_packet
        if (mm.length() == 7) {
            mm.position(5);
            int optCommand = mm.readUB2();
            if (optCommand == 0) {
                multiStatementAllow = true;
                write(EOFPacket.getDefault());
                return;
            } else if (optCommand == 1) {
                multiStatementAllow = false;
                write(EOFPacket.getDefault());
                return;
            }
        }
        writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Set Option ERROR!");
    }

    public abstract void resetConnection();

    public abstract void handleVariable(MysqlVariable variable);

    public abstract Session getSession();

}
