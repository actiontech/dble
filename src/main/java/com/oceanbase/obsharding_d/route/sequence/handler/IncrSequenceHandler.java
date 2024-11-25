/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.route.sequence.handler;

import com.oceanbase.obsharding_d.config.util.ConfigException;
import com.oceanbase.obsharding_d.services.FrontendService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * IncrSequenceHandler
 *
 * @author <a href="http://www.micmiu.com">Michael</a>
 * @version 1.0
 * @time Create on 2013-12-29 22:42:39
 */
public abstract class IncrSequenceHandler implements SequenceHandler {

    public static final Logger LOGGER = LoggerFactory.getLogger(IncrSequenceHandler.class);

    public static final String KEY_MIN_NAME = ".MINID"; // 1
    public static final String KEY_MAX_NAME = ".MAXID"; // 10000
    public static final String KEY_CUR_NAME = ".CURID"; // 888

    public abstract Object[] getParaValMap(String prefixName);

    public abstract Boolean updateCurIDVal(String prefixName, Long val);

    public abstract Boolean fetchNextPeriod(String prefixName);

    @Override
    public synchronized long nextId(String prefixName, @Nullable FrontendService frontendService) {
        Object[] objects = this.getParaValMap(prefixName);
        if (null == objects) {
            String msg = "can't find definition for sequence :" + prefixName;
            LOGGER.info(msg);
            throw new ConfigException(msg);
        }
        String prefixTable = (String) objects[0];
        Map<String, String> paraMap = (Map<String, String>) objects[1];
        long nextId = Long.parseLong(paraMap.get(prefixTable + KEY_CUR_NAME)) + 1;
        long maxId = Long.parseLong(paraMap.get(prefixTable + KEY_MAX_NAME));
        if (nextId > maxId) {
            fetchNextPeriod(prefixTable);
            return nextId(prefixTable, frontendService);
        }
        updateCurIDVal(prefixTable, nextId);
        return nextId;

    }

}
