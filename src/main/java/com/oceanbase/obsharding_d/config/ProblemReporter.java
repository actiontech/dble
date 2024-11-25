/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.config;

public interface ProblemReporter {
    void error(String problem);

    void warn(String problem);

    void notice(String problem);
}
