package com.actiontech.dble.services;

import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.CharsetNames;
import com.actiontech.dble.server.handler.SetHandler;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by szf on 2020/6/28.
 */
public abstract class MySQLVariablesService extends MySQLBasedService {

    protected volatile Map<String, String> usrVariables = new LinkedHashMap<>();
    protected volatile Map<String, String> sysVariables = new LinkedHashMap<>();

    protected volatile int txIsolation;
    protected volatile boolean autocommit;

    public MySQLVariablesService(AbstractConnection connection) {
        super(connection);
    }

    public void executeContextSetTask(SetHandler.SetItem[] contextTask) {
        SetHandler.SetItem autocommitItem = null;
        for (SetHandler.SetItem setItem : contextTask) {
            switch (setItem.getType()) {
                case CHARACTER_SET_CLIENT:
                    String charsetClient = setItem.getValue();
                    this.setCharacterClient(charsetClient);
                    break;
                case CHARACTER_SET_CONNECTION:
                    String collationName = setItem.getValue();
                    this.setCharacterConnection(collationName);
                    break;
                case CHARACTER_SET_RESULTS:
                    String charsetResult = setItem.getValue();
                    this.setCharacterResults(charsetResult);
                    break;
                case COLLATION_CONNECTION:
                    String collation = setItem.getValue();
                    this.setCollationConnection(collation);
                    break;
                case TX_ISOLATION:
                    String isolationLevel = setItem.getValue();
                    this.setTxIsolation(Integer.parseInt(isolationLevel));
                    break;
                case SYSTEM_VARIABLES:
                    this.sysVariables.put(setItem.getName(), setItem.getValue());
                    break;
                case USER_VARIABLES:
                    if (setItem.getValue() != null) {
                        this.usrVariables.put(setItem.getName(), setItem.getValue());
                    }
                    break;
                case CHARSET:
                    this.setCharacterSet(setItem.getValue());
                    break;
                case NAMES:
                    String[] charsetAndCollate = setItem.getValue().split(":");
                    this.setNames(charsetAndCollate[0], charsetAndCollate[1]);
                    break;
                case AUTOCOMMIT:
                    autocommitItem = setItem;
                    break;
                default:
                    handleSetItem(setItem);
                    break;
            }
        }

        if (autocommitItem == null) {
            writeOkPacket();
        } else {
            handleSetItem(autocommitItem);
        }
    }

    public abstract void handleSetItem(SetHandler.SetItem setItem);

    public void setCollationConnection(String collation) {
        connection.getCharsetName().setCollation(collation);
    }

    public void setCharacterResults(String name) {
        connection.getCharsetName().setResults(name);
    }

    public void setCharacterConnection(String collationName) {
        connection.getCharsetName().setCollation(collationName);
    }

    public void setNames(String name, String collationName) {
        connection.getCharsetName().setNames(name, collationName);
    }

    public void setCharacterClient(String name) {
        connection.getCharsetName().setClient(name);
    }

    public int getTxIsolation() {
        return txIsolation;
    }

    public void setTxIsolation(int txIsolation) {
        this.txIsolation = txIsolation;
    }

    public void setCharacterSet(String name) {
        connection.setCharacterSet(name);
    }

    public CharsetNames getCharset() {
        return connection.getCharsetName();
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    public void setAutocommit(boolean autocommit) {
        this.autocommit = autocommit;
    }

    public Map<String, String> getSysVariables() {
        return sysVariables;
    }

    public Map<String, String> getUsrVariables() {
        return usrVariables;
    }

    public String getStringOfSysVariables() {
        StringBuilder sbSysVariables = new StringBuilder();
        int cnt = 0;
        if (sysVariables != null) {
            for (Map.Entry<String, String> entry : sysVariables.entrySet()) {
                if (cnt > 0) {
                    sbSysVariables.append(",");
                }
                sbSysVariables.append(entry.getKey());
                sbSysVariables.append("=");
                sbSysVariables.append(entry.getValue());
                cnt++;
            }
        }
        return sbSysVariables.toString();
    }

    public String getStringOfUsrVariables() {
        StringBuilder sbUsrVariables = new StringBuilder();
        int cnt = 0;
        if (usrVariables != null) {
            for (Map.Entry<String, String> entry : usrVariables.entrySet()) {
                if (cnt > 0) {
                    sbUsrVariables.append(",");
                }
                sbUsrVariables.append(entry.getKey());
                sbUsrVariables.append("=");
                sbUsrVariables.append(entry.getValue());
                cnt++;

            }
        }
        return sbUsrVariables.toString();
    }


}
