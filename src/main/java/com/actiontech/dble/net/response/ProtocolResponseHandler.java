package com.actiontech.dble.net.response;

public interface ProtocolResponseHandler {

    int HEADER = 0;
    int FIELD = 1;
    int ROW = 2;

    int PREPARED_PARAM = 3;
    int PREPARED_FIELD = 4;

    void ok(byte[] data);

    void error(byte[] data);

    void eof(byte[] data);

    void data(byte[] data);

}
