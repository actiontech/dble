/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.variables;

import com.actiontech.dble.config.ServerConfig;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SysVarsExtractor {
    private static final Logger LOGGER = LoggerFactory.getLogger(SysVarsExtractor.class);

    private final ServerConfig config;

    public SysVarsExtractor(ServerConfig config) {
        this.config = config;
    }

    public void extract() {
        VarsExtractorHandler handler = new VarsExtractorHandler();
        handler.execute();
        return;
    }
}
