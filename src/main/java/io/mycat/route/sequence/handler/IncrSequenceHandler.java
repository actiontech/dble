/*
 * Copyright (c) 2013, OpenCloudDB/MyCAT and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software;Designed and Developed mainly by many Chinese
 * opensource volunteers. you can redistribute it and/or modify it under the
 * terms of the GNU General Public License version 2 only, as published by the
 * Free Software Foundation.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Any questions about this component can be directed to it's project Web address
 * https://code.google.com/p/opencloudb/.
 *
 */
package io.mycat.route.sequence.handler;

import io.mycat.config.util.ConfigException;
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

    public static final String FILE_NAME = "sequence_conf.properties";

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
            LOGGER.warn(msg);
            throw new ConfigException(msg);
        }
        Long nextId = Long.parseLong(paraMap.get(prefixName + KEY_CUR_NAME)) + 1;
        Long maxId = Long.parseLong(paraMap.get(prefixName + KEY_MAX_NAME));
        if (nextId > maxId) {
            fetchNextPeriod(prefixName);
            return nextId(prefixName);
        }
        updateCurIDVal(prefixName, nextId);
        return nextId;

    }
}
