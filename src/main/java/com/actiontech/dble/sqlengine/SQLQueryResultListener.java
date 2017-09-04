package com.actiontech.dble.sqlengine;


public interface SQLQueryResultListener<T> {

    void onResult(T result);

}
