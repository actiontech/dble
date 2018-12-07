package com.actiontech.dble.config;

public interface ProblemReporter {
    void error(String problem);

    void warn(String problem);
}
