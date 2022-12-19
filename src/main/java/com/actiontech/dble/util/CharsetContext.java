package com.actiontech.dble.util;

import java.util.Objects;

public final class CharsetContext {
    private CharsetContext() {
    }

    private static final ThreadLocal<String> CHARSET_CTX = new ThreadLocal<>();

    public static void put(String value) {
        CHARSET_CTX.set(value);
    }

    public static String get() {
        return CHARSET_CTX.get();
    }

    public static String remove() {
        String value = get();
        if (Objects.nonNull(value)) {
            CHARSET_CTX.remove();
        }
        return value;
    }
}
