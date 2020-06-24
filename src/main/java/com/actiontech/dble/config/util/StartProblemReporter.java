/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.util;

import com.actiontech.dble.config.ProblemReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class StartProblemReporter implements ProblemReporter {
    private static final Logger LOGGER = LoggerFactory.getLogger(StartProblemReporter.class);
    private static final StartProblemReporter INSTANCE = new StartProblemReporter();

    public static StartProblemReporter getInstance() {
        return INSTANCE;
    }

    private StartProblemReporter() {
    }

    @Override
    public void error(String problem) {
        LOGGER.warn(problem);
        throw new ConfigException(problem);
    }

    @Override
    public void warn(String problem) {
        LOGGER.warn(problem);
        throw new ConfigException(problem);
    }

    @Override
    public void notice(String problem) {
        LOGGER.info(problem);
    }
}
