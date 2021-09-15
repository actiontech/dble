/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.singleton;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.SystemConfig;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.regex.Pattern;

/**
 * @author dcy
 * Create Date: 2021-09-14
 */
public final class RoutePenetrationManager {
    private static final RoutePenetrationManager INSTANCE = new RoutePenetrationManager();
    private final List<PenetrationRule> list = Lists.newArrayList();
    private static final Logger LOGGER = LogManager.getLogger(RoutePenetrationManager.class);

    private RoutePenetrationManager() {
    }

    public static RoutePenetrationManager getInstance() {
        return INSTANCE;
    }


    public void init() {
        final SystemConfig config = DbleServer.getInstance().getConfig().getSystem();
        try {
            if (config.isEnableRoutePenetration()) {
                final String routePenetrationRules = config.getRoutePenetrationRules();
                //split with ;  ,exclude the \;
                final String[] rules = routePenetrationRules.trim().split("(?<!\\\\);");
                if (rules.length % 2 != 0) {
                    throw new IllegalStateException("rule must be pairwise.");
                }
                for (int i = 0; i + 1 < rules.length; i += 2) {
                    String regex = rules[i];
                    //escape \; to ;
                    regex = regex.replaceAll("\\\\;", ";");
                    final boolean caseSensitive = (Integer.parseInt(rules[i + 1]) & 1) != 0;
                    int flag = 0;
                    if (!caseSensitive) {
                        flag |= Pattern.CASE_INSENSITIVE;
                    }
                    final Pattern pattern = Pattern.compile(regex, flag);
                    list.add(new PenetrationRule(pattern));
                }
            }
        } catch (Exception e) {
            LOGGER.error("can't parse the sql-penetration rule, detail exception is ", e);
            throw e;
        }
    }

    public boolean isEnabled() {
        return DbleServer.getInstance().getConfig().getSystem().isEnableRoutePenetration();
    }

    public boolean match(String sql) {
        return list.stream().anyMatch((rule) -> rule.match(sql));
    }

    private static final class PenetrationRule {
        private final Pattern pattern;

        private PenetrationRule(Pattern pattern) {
            this.pattern = pattern;
        }

        public boolean match(String sql) {
            return pattern.matcher(sql).matches();
        }
    }
}
