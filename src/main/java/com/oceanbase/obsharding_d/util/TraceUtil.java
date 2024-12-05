/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class TraceUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(TraceUtil.class);

    private TraceUtil() {
    }


    public static void printLocation(Object context) {
        if (LOGGER.isTraceEnabled()) {
            try {
                throw new DebugPrinter();
            } catch (DebugPrinter e) {
                LOGGER.trace("location:, context:{}", context, e);
            }

        }
    }


    @SuppressWarnings("AlibabaExceptionClassShouldEndWithException")
    public static class DebugPrinter extends Exception {
        public DebugPrinter(String message) {
            super(message);
        }

        public DebugPrinter() {
            super("used for debugger");
        }
    }

}
