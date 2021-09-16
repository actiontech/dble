/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.actiontech.dble.singleton;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.config.model.SystemConfig;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author dcy
 * Create Date: 2021-09-14
 */
public final class RoutePenetrationManager {
    private static final RoutePenetrationManager INSTANCE = new RoutePenetrationManager();
    private List<PenetrationRule> rules = Lists.newArrayList();
    private static final Logger LOGGER = LogManager.getLogger(RoutePenetrationManager.class);

    private RoutePenetrationManager() {
    }

    public static RoutePenetrationManager getInstance() {
        return INSTANCE;
    }


    public void init() {
        final SystemConfig config = DbleServer.getInstance().getConfig().getSystem();
        try {
            Gson gson = new Gson();
            if (config.isEnableRoutePenetration()) {
                final String routePenetrationRules = config.getRoutePenetrationRules();
                rules = gson.fromJson(routePenetrationRules, new TypeToken<List<PenetrationRule>>() {
                }.getType());
                rules.forEach(PenetrationRule::init);
            }
            LOGGER.info("init {}  route-penetration rules success", rules.size());
        } catch (Exception e) {
            LOGGER.error("can't parse the route-penetration rule, detail exception is " + e);
            throw e;
        }
    }

    public boolean isEnabled() {
        return DbleServer.getInstance().getConfig().getSystem().isEnableRoutePenetration();
    }

    public boolean match(String sql) {
        return rules.stream().anyMatch((rule) -> rule.match(sql));
    }

    private static final class PenetrationRule {
        private Pattern pattern;
        private String regex;
        private boolean caseSensitive = true;
        private boolean partMatch = true;

        public String getRegex() {
            return regex;
        }

        public void setRegex(String regex) {
            this.regex = regex;
        }

        public boolean isCaseSensitive() {
            return caseSensitive;
        }

        public void setCaseSensitive(boolean caseSensitive) {
            this.caseSensitive = caseSensitive;
        }

        public boolean isPartMatch() {
            return partMatch;
        }

        public void setPartMatch(boolean partMatch) {
            this.partMatch = partMatch;
        }


        public void init() {
            int flag = 0;
            if (!caseSensitive) {
                flag |= Pattern.CASE_INSENSITIVE;
            }
            pattern = Pattern.compile(regex, flag);
        }

        public boolean match(String sql) {
            final Matcher matcher = pattern.matcher(sql);
            return partMatch ? matcher.find() : matcher.matches();
        }

        @Override
        public String toString() {
            return "PenetrationRule{" +
                    ", regex='" + regex + '\'' +
                    ", caseSensitive=" + caseSensitive +
                    ", partMatch=" + partMatch +
                    '}';
        }
    }
}
