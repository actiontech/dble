/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.sqlengine;


public interface SQLQueryResultListener<T> {

    void onResult(T result);

}
