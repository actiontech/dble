package com.actiontech.dble.services;

import com.actiontech.dble.backend.mysql.VersionUtil;
import com.actiontech.dble.config.Isolations;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.CharsetNames;
import com.actiontech.dble.server.variables.MysqlVariable;
import com.actiontech.dble.server.variables.VariableType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
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

    public void executeContextSetTask(MysqlVariable[] contextTask) {
        MysqlVariable autocommitVariable = null;
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
                    autocommitVariable = variable;
                    break;
                default:
                    handleVariable(variable);
                    break;
            }
        }

        if (autocommitVariable == null) {
            writeOkPacket();
        } else {
            handleVariable(autocommitVariable);
        }
    }

    public abstract void handleVariable(MysqlVariable variable);

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

    public List<MysqlVariable> getAllVars() {
        List<MysqlVariable> variables = new ArrayList<>();

        variables.add(new MysqlVariable("autocommit", autocommit + "", VariableType.SYSTEM_VARIABLES));
        variables.add(new MysqlVariable("character_set_client", connection.getCharsetName().getClient(), VariableType.SYSTEM_VARIABLES));
        variables.add(new MysqlVariable("collation_connection", connection.getCharsetName().getCollation(), VariableType.SYSTEM_VARIABLES));
        variables.add(new MysqlVariable("character_set_results", connection.getCharsetName().getResults(), VariableType.SYSTEM_VARIABLES));
        variables.add(new MysqlVariable("character_set_connection", connection.getCharsetName().getCollation(), VariableType.SYSTEM_VARIABLES));
        variables.add(new MysqlVariable(VersionUtil.TRANSACTION_ISOLATION, Isolations.getIsolation(txIsolation), VariableType.SYSTEM_VARIABLES));

        if (sysVariables != null) {
            for (Map.Entry<String, String> entry : sysVariables.entrySet()) {
                variables.add(new MysqlVariable(entry.getKey(), entry.getValue(), VariableType.SYSTEM_VARIABLES));
            }
        }
        if (usrVariables != null) {
            for (Map.Entry<String, String> entry : usrVariables.entrySet()) {
                variables.add(new MysqlVariable(entry.getKey(), entry.getValue(), VariableType.USER_VARIABLES));
            }
        }
        return variables;
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
