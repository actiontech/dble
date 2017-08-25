package io.mycat.plan.common.ptr;

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
