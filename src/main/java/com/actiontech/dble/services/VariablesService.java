package com.actiontech.dble.services;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import com.actiontech.dble.backend.mysql.VersionUtil;
import com.actiontech.dble.config.Isolations;
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
public class VariablesService {

    protected final Map<String, String> usrVariables;
    protected final Map<String, String> sysVariables;
    protected final CharsetNames charsetName;

    protected volatile int txIsolation;
    protected volatile boolean autocommit;
    protected volatile boolean multiStatementAllow;

    public VariablesService() {
        this.usrVariables = new LinkedHashMap<>();
        this.sysVariables = new LinkedHashMap<>();
        this.charsetName = new CharsetNames();
    }

    public List<MysqlVariable> getAllVars() {
        List<MysqlVariable> variables = new ArrayList<>();

        variables.add(new MysqlVariable("autocommit", autocommit + "", VariableType.SYSTEM_VARIABLES));
        variables.add(new MysqlVariable("character_set_client", charsetName.getClient(), VariableType.SYSTEM_VARIABLES));
        variables.add(new MysqlVariable("collation_connection", charsetName.getCollation(), VariableType.SYSTEM_VARIABLES));
        variables.add(new MysqlVariable("character_set_results", charsetName.getResults(), VariableType.SYSTEM_VARIABLES));
        variables.add(new MysqlVariable("character_set_connection", charsetName.getCollation(), VariableType.SYSTEM_VARIABLES));
        variables.add(new MysqlVariable(VersionUtil.TRANSACTION_ISOLATION, Isolations.getIsolation(txIsolation), VariableType.SYSTEM_VARIABLES));

        for (Map.Entry<String, String> entry : sysVariables.entrySet()) {
            variables.add(new MysqlVariable(entry.getKey(), entry.getValue(), VariableType.SYSTEM_VARIABLES));
        }

        for (Map.Entry<String, String> entry : usrVariables.entrySet()) {
            variables.add(new MysqlVariable(entry.getKey(), entry.getValue(), VariableType.USER_VARIABLES));
        }
        return variables;
    }

    // charset
    public CharsetNames getCharset() {
        return charsetName;
    }

    public CharsetNames getCharsetName() {
        return charsetName;
    }

    public void initCharacterSet(String name) {
        charsetName.setClient(name);
        charsetName.setResults(name);
        charsetName.setCollation(CharsetUtil.getDefaultCollation(name));
    }

    public void initCharsetIndex(int ci) {
        String name = CharsetUtil.getCharset(ci);
        if (name != null) {
            charsetName.setClient(name);
            charsetName.setResults(name);
            charsetName.setCollation(CharsetUtil.getDefaultCollation(name));
        }
    }

    public void setCharacterSet(String name) {
        charsetName.setClient(name);
        charsetName.setResults(name);
        charsetName.setCollation(DbleServer.getInstance().getSystemVariables().getDefaultValue("collation_database"));
    }

    public void setCharsetName(CharsetNames charName) {
        charsetName.setCollation(charName.getCollation());
        charsetName.setResults(charName.getResults());
        charsetName.setClient(charName.getClient());
    }

    public void setCollationConnection(String collation) {
        charsetName.setCollation(collation);
    }

    public void setCharacterResults(String name) {
        charsetName.setResults(name);
    }

    public void setCharacterConnection(String collationName) {
        charsetName.setCollation(collationName);
    }

    public void setNames(String name, String collationName) {
        charsetName.setNames(name, collationName);
    }

    public void setCharacterClient(String name) {
        charsetName.setClient(name);
    }

    public Map<String, String> getUsrVariables() {
        return usrVariables;
    }

    public Map<String, String> getSysVariables() {
        return sysVariables;
    }

    // isolation
    public int getTxIsolation() {
        return txIsolation;
    }

    public void setTxIsolation(int txIsolation) {
        this.txIsolation = txIsolation;
    }

    // autocommit
    public boolean isAutocommit() {
        return autocommit;
    }

    public void setAutocommit(boolean autocommit) {
        this.autocommit = autocommit;
    }

    // multiStatementAllow
    public boolean isMultiStatementAllow() {
        return multiStatementAllow;
    }

    public void setMultiStatementAllow(boolean multiStatementAllow) {
        this.multiStatementAllow = multiStatementAllow;
    }

    public String getStringOfSysVariables() {
        StringBuilder sbSysVariables = new StringBuilder(50);
        int cnt = 0;
        for (Map.Entry<String, String> entry : sysVariables.entrySet()) {
            if (cnt > 0) {
                sbSysVariables.append(",");
            }
            sbSysVariables.append(entry.getKey());
            sbSysVariables.append("=");
            sbSysVariables.append(entry.getValue());
            cnt++;
        }
        return sbSysVariables.toString();
    }

    public String getStringOfUsrVariables() {
        StringBuilder sbUsrVariables = new StringBuilder(50);
        int cnt = 0;
        for (Map.Entry<String, String> entry : usrVariables.entrySet()) {
            if (cnt > 0) {
                sbUsrVariables.append(",");
            }
            sbUsrVariables.append(entry.getKey());
            sbUsrVariables.append("=");
            sbUsrVariables.append(entry.getValue());
            cnt++;

        }
        return sbUsrVariables.toString();
    }

}
