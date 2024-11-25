/*
 * Copyright (C) 2016-2021 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.singleton;

import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.google.common.collect.Lists;
import com.google.gson.*;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.reflect.Type;
import java.util.Arrays;
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
        final SystemConfig config = SystemConfig.getInstance();
        try {
            final JsonBooleanDeserializer deserializer = new JsonBooleanDeserializer();
            final Gson gson = new GsonBuilder().registerTypeAdapter(Boolean.class, deserializer).registerTypeAdapter(boolean.class, deserializer).create();
            if (config.isEnableRoutePenetration() == 1) {
                final String routePenetrationRules = config.getRoutePenetrationRules();
                if (StringUtils.isBlank(routePenetrationRules)) {
                    throw new IllegalStateException("property routePenetrationRules can't be null");
                }
                final PenetrationConfig penetrationConfig = gson.fromJson(routePenetrationRules, PenetrationConfig.class);
                if (penetrationConfig.getRules() == null) {
                    throw new IllegalStateException("rules can't be null");
                }
                rules = Arrays.asList(penetrationConfig.getRules());
                rules.forEach(PenetrationRule::init);
            }
            LOGGER.info("init {}  route-penetration rules success", rules.size());
            if (LOGGER.isDebugEnabled())
                LOGGER.debug("route-penetration rules :{}", rules);
        } catch (Exception e) {
            final String msg = "can't parse the route-penetration rule, please check the 'routePenetrationRules', detail exception is :" + e;
            LOGGER.error(msg);
            throw new IllegalStateException("The system property routePenetrationRules in bootstrap.cnf is illegal or unset, for more detail, please check OBsharding-D.log .");
        }
    }

    public boolean isEnabled() {
        return SystemConfig.getInstance().isEnableRoutePenetration() == 1;
    }

    public boolean match(String sql) {
        return rules.stream().anyMatch((rule) -> rule.match(sql));
    }

    private static final class PenetrationConfig {

        private PenetrationRule[] rules = new PenetrationRule[0];

        public PenetrationRule[] getRules() {
            return rules;
        }

        public void setRules(PenetrationRule[] rules) {
            this.rules = rules;
        }
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
            if (StringUtils.isBlank(regex)) {
                throw new IllegalStateException("regex can't be null or empty.");
            }
            int flag = Pattern.DOTALL;
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

    private static class JsonBooleanDeserializer implements JsonDeserializer<Boolean> {
        @Override
        public Boolean deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            try {
                String value = json.getAsJsonPrimitive().getAsString();
                if (value != null) {
                    value = value.toLowerCase();
                }
                if ("true".equals(value) || "false".equals(value)) {
                    return Boolean.valueOf(value);
                } else {
                    throw new JsonParseException("Cannot parse json '" + json.toString() + "' to boolean value");
                }
            } catch (Exception e) {
                throw new JsonParseException("Cannot parse json '" + json.toString() + "' to boolean value", e);
            }
        }
    }
}
