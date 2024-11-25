/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.nio.handler.transaction;

import com.oceanbase.obsharding_d.net.mysql.MySQLPacket;

public interface TransactionStage {

    void onEnterStage();

    TransactionStage next(boolean isFail, String errMsg, MySQLPacket errPacket);

}
