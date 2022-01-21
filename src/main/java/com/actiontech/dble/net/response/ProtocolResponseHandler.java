/*
 * Copyright (C) 2016-2022 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.response;

public interface ProtocolResponseHandler {

    int INITIAL = -1;

    int FIELD = 1;
    int ROW = 2;

    int PREPARED_PARAM = 3;
    int PREPARED_FIELD = 4;

    void ok(byte[] data);

    void error(byte[] data);

    void eof(byte[] data);

    void data(byte[] data);

}
