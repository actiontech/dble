/*
* Copyright (C) 2016-2020 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.route.sequence.handler;

import com.actiontech.dble.config.util.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public abstract Map<String, String> getParaValMap(String prefixName);

    public abstract Boolean updateCurIDVal(String prefixName, Long val);

    public abstract Boolean fetchNextPeriod(String prefixName);

    @Override
    public synchronized long nextId(String prefixName) {
        Map<String, String> paraMap = this.getParaValMap(prefixName);
        if (null == paraMap) {
            String msg = "can't find definition for sequence :" + prefixName;
            LOGGER.info(msg);
            throw new ConfigException(msg);
        }
        long nextId = Long.parseLong(paraMap.get(prefixName + KEY_CUR_NAME)) + 1;
        long maxId = Long.parseLong(paraMap.get(prefixName + KEY_MAX_NAME));
        if (nextId > maxId) {
            fetchNextPeriod(prefixName);
            return nextId(prefixName);
        }
        updateCurIDVal(prefixName, nextId);
        return nextId;

    }

}
