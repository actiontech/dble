/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.mysql;

import com.oceanbase.obsharding_d.OBsharding_DServer;
import com.oceanbase.obsharding_d.backend.mysql.CharsetUtil;
import org.apache.commons.lang.StringUtils;

public class CharsetNames {
    private volatile String client;
    private volatile String results;
    private volatile String collation;

    public CharsetNames() {
        this.client = OBsharding_DServer.getInstance().getSystemVariables().getDefaultValue("character_set_client");
        this.results = OBsharding_DServer.getInstance().getSystemVariables().getDefaultValue("character_set_results");
        this.collation = OBsharding_DServer.getInstance().getSystemVariables().getDefaultValue("collation_connection");
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

}
