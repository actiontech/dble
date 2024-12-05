/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.services.rwsplit;

public interface Callback {

    void callback(boolean isSuccess, byte[] response, RWSplitService rwSplitService);

}
