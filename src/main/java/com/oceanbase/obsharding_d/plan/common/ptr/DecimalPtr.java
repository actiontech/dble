/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.plan.common.ptr;

import java.math.BigDecimal;

public class DecimalPtr {
    private BigDecimal bd;

    public DecimalPtr(BigDecimal bd) {
        this.bd = bd;
    }

    public BigDecimal get() {
        return bd;
    }

    public void set(BigDecimal bigDecimal) {
        this.bd = bigDecimal;
    }
}
