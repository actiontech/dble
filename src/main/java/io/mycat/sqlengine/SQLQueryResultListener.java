package io.mycat.sqlengine;


public interface SQLQueryResultListener<T> {

    void onResult(T result);

}
