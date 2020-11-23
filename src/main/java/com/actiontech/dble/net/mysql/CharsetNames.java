/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.mysql;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.mysql.CharsetUtil;
import org.apache.commons.lang.StringUtils;

public class CharsetNames {
    private volatile String client;
    private volatile String results;
    private volatile String collation;

    public CharsetNames() {
        this.client = DbleServer.getInstance().getSystemVariables().getDefaultValue("character_set_client");
        this.results = DbleServer.getInstance().getSystemVariables().getDefaultValue("character_set_results");
        this.collation = DbleServer.getInstance().getSystemVariables().getDefaultValue("collation_connection");
    }

    public void setNames(String name, String collationName) {
        this.client = name;
        this.results = name;
        this.collation = collationName;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }
    public String getResults() {
        return results;
    }

    public void setResults(String results) {
        this.results = results;
    }

    public int getResultsIndex() {
        return CharsetUtil.getCharsetDefaultIndex(results);
    }

    public String getCollation() {
        return collation;
    }

    public void setCollation(String collation) {
        this.collation = collation;
    }
    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof CharsetNames))
            return false;
        CharsetNames other = (CharsetNames) obj;
        return StringUtils.equalsIgnoreCase(client, other.client) &&
                StringUtils.equalsIgnoreCase(results, other.results) &&
                StringUtils.equalsIgnoreCase(collation, other.collation);
    }
    @Override
    public int hashCode() {
        // should not use
        return client.hashCode();
    }

    @Override
    public String toString() {
        return "character_set_client=" + client +
                ",character_set_results=" + results + ",collation_connection=" + collation;
    }

    public CharsetNames copyObj() {
        CharsetNames obj = new CharsetNames();
        obj.client = this.client;
        obj.results = this.results;
        obj.collation = this.collation;
        return obj;
    }
}
