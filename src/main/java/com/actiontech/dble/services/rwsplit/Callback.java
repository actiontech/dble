package com.actiontech.dble.services.rwsplit;

public interface Callback {

    void callback(boolean isSuccess, byte[] response, RWSplitService rwSplitService);

}
