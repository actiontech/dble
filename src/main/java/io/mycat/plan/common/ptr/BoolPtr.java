package io.mycat.plan.common.ptr;

public class BoolPtr {
    private boolean b;

    public BoolPtr(boolean b) {
        this.b = b;
    }

    public boolean get() {
        return b;
    }

    public void set(boolean b) {
        this.b = b;
    }
}
