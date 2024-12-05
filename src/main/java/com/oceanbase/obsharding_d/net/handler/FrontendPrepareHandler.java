/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.net.handler;

/**
 * FrontendPrepareHandler
 *
 * @author mycat, CrazyPig
 */
public interface FrontendPrepareHandler {

    void prepare(String sql);

    void sendLongData(byte[] data);

    void reset(byte[] data);

    void execute(byte[] data);

    void close(byte[] data);

    void clear();

    void fetch(byte[] data);

}
