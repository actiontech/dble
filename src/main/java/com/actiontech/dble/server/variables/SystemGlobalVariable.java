/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.server.variables;

public interface SystemGlobalVariable {
    void setVariable(String value, SystemVariables sys) throws RuntimeException;
    String getVariable();
}
