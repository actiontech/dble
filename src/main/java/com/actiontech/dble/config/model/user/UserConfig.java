/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.model.user;

import com.actiontech.dble.util.SplitUtil;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class UserConfig {
    protected final String name;
    protected final String password;
    protected final Set<String> whiteIPs;

    public UserConfig(String name, String password, String strWhiteIPs) {
        this.name = name;
        this.password = password;
        this.whiteIPs = genWhiteIPs(strWhiteIPs);
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

}
