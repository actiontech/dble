/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services;

import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.server.variables.MysqlVariable;

import java.util.concurrent.atomic.AtomicLong;

public abstract class BusinessService extends FrontEndService {
    protected volatile boolean txStarted;
    protected final AtomicLong queriesCounter = new AtomicLong(0);
    protected final AtomicLong transactionsCounter = new AtomicLong(0);

    public BusinessService(AbstractConnection connection) {
        super(connection);
    }


    public boolean isTxStart() {
        return txStarted;
    }

    public void setTxStart(boolean txStart) {
        this.txStarted = txStart;
    }

    public void queryCount() {
        queriesCounter.incrementAndGet();
    }

    public void transactionsCount() {
        transactionsCounter.incrementAndGet();
    }

    public void singleTransactionsCount() {
        if (!this.isTxStart()) {
            transactionsCounter.incrementAndGet();
        }
    }


    public long getQueriesCounter() {
        return queriesCounter.get();
    }

    public long getTransactionsCounter() {
        return transactionsCounter.get();
    }

    public void resetCounter() {
        queriesCounter.set(Long.MIN_VALUE);
        transactionsCounter.set(Long.MIN_VALUE);
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
                case SYSTEM_VARIABLES:
                    this.sysVariables.put(variable.getName(), variable.getValue());
                    break;
                case USER_VARIABLES:
                    if (variable.getValue() != null) {
                        this.usrVariables.put(variable.getName(), variable.getValue());
                    }
                    break;
                case CHARSET:
                    this.setCharacterSet(variable.getValue());
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
            this.singleTransactionsCount();
            writeOkPacket();
        } else {
            handleVariable(autocommitItem);
        }
    }
    public abstract void handleVariable(MysqlVariable variable);
}
