/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.user;

import com.actiontech.dble.util.SplitUtil;
import com.actiontech.dble.util.StringUtil;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public abstract class UserConfig {
    protected final String name;
    protected final String password;
    protected final Set<String> whiteIPs;
    protected int maxCon;

    public UserConfig(String name, String password, String strWhiteIPs, String strMaxCon) {
        this.name = name;
        this.password = password;
        this.whiteIPs = genWhiteIPs(strWhiteIPs);

        int maxConn = -1;
        if (!StringUtil.isEmpty(strMaxCon)) {
            maxConn = Integer.parseInt(strMaxCon);
            if (maxConn < 0) {
                maxConn = -1;
            }
        }
        this.maxCon = maxConn;
    }

    private Set<String> genWhiteIPs(String strWhiteIPs) {
        Set<String> result = new HashSet<>();
        if (strWhiteIPs != null) {
            String[] theWhiteIPs = SplitUtil.split(strWhiteIPs, ',', '$', '-');
            result.addAll(Arrays.asList(theWhiteIPs));
        }
        return result;
    }

    public String getName() {
        return name;
    }


    public String getPassword() {
        return password;
    }


    public Set<String> getWhiteIPs() {
        return whiteIPs;
    }


    public int getMaxCon() {
        return maxCon;
    }
}
