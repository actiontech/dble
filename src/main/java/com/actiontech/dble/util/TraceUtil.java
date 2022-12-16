package com.actiontech.dble.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public final class TraceUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(TraceUtil.class);

    private TraceUtil() {
    }

    public static void print() {
        LOGGER.warn(printStackTrace());
    }

    public static synchronized String printStackTrace() {
        Throwable throwable = new Throwable();
        StackTraceElement[] stackElements = throwable.getStackTrace();
        StringBuilder sb = new StringBuilder();
        String line = "\r\n";
        if (Objects.nonNull(stackElements)) {
            sb.append("start stack trace").append(line);
            for (int i = 0; i < stackElements.length; i++) {
                sb.append(stackElements[i].getClassName());
                sb.append(".").append(stackElements[i].getMethodName());
                sb.append("(").append(stackElements[i].getFileName()).append(":");
                sb.append(stackElements[i].getLineNumber() + ")").append(line);
            }
            sb.append("end stack trace").append(line);
        }
        return sb.toString();
    }
}
