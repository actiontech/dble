package com.actiontech.dble.services;

import com.actiontech.dble.backend.mysql.VersionUtil;
import com.actiontech.dble.config.Isolations;
import com.actiontech.dble.config.model.SystemConfig;
import com.actiontech.dble.net.connection.AbstractConnection;
import com.actiontech.dble.net.mysql.CharsetNames;
import com.actiontech.dble.net.service.AbstractService;
import com.actiontech.dble.server.variables.MysqlVariable;
import com.actiontech.dble.server.variables.VariableType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by szf on 2020/6/28.
 */
public abstract class VariablesService extends AbstractService {

    protected volatile Map<String, String> usrVariables = new LinkedHashMap<>();
    protected volatile Map<String, String> sysVariables = new LinkedHashMap<>();

    protected volatile int txIsolation;
    protected volatile boolean autocommit;

    public VariablesService(AbstractConnection connection) {
        super(connection);
        this.autocommit = SystemConfig.getInstance().getAutocommit() == 1;
        this.txIsolation = SystemConfig.getInstance().getTxIsolation();
    }

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
