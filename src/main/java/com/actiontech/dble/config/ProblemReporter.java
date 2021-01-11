/*
 * Copyright (C) 2016-2021 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config;

public interface ProblemReporter {
    void error(String problem);

    void warn(String problem);

    void notice(String problem);
}
