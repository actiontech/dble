/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util.exception;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Properties;


public final class TmpFileException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private static final Properties MESSAGES = new Properties();

    static {
        MESSAGES.put("5301", "Could not open file {0}");
        MESSAGES.put("5302", "Could not force file {0}");
        MESSAGES.put("5303", "Could not sync file {0}");
        MESSAGES.put("5304", "Reading from {0} failed");
        MESSAGES.put("5305", "Writing to {0} failed");
        MESSAGES.put("5306", "Error while renaming file {0} to {1}");
        MESSAGES.put("5307", "Cannot delete file {0}");
        MESSAGES.put("5308", "IO Exception: {0}");
        MESSAGES.put("5309", "Reading from {0} failed,index out of bounds");
        MESSAGES.put("5310", "Hex a decimal string with odd number of characters: {0}");
        MESSAGES.put("5311", "Hex a decimal string contains non-hex character: {0}");
        MESSAGES.put("5312", "Invalid value {0} for parameter {1}");
        MESSAGES.put("5313", "Error while creating file {0}");
    }

    private TmpFileException(String message) {
        super(message, null);
    }

    private TmpFileException(String message, Throwable cause) {
        super(message, cause);
    }

    public static TmpFileException get(int errorCode, String... params) {
        String message = translate(String.valueOf(errorCode), params);
        return new TmpFileException(message);
    }

    public static TmpFileException get(int errorCode, Throwable cause, String... params) {
        String message = translate(String.valueOf(errorCode), params);
        return new TmpFileException(message, cause);
    }

    /**
     * Create a ares exception for a specific error code.
     *
     * @param errorCode the error code
     * @param p1        the first parameter of the message
     * @return the exception
     */
    public static TmpFileException get(int errorCode, String p1) {
        return get(errorCode, new String[]{p1});
    }

    private static String translate(String key, String... params) {
        String message = MESSAGES.getProperty(key);
        if (message == null) {
            message = "(Message " + key + " not found)";
        }
        if (params != null) {
            for (int i = 0; i < params.length; i++) {
                String s = params[i];
                if (s != null && s.length() > 0) {
                    params[i] = quoteIdentifier(s);
                }
            }
            message = MessageFormat.format(message, (Object[]) params);
        }
        return message;
    }

    private static String quoteIdentifier(String s) {
        int length = s.length();
        StringBuilder buff = new StringBuilder(length + 2);
        buff.append('\"');
        for (int i = 0; i < length; i++) {
            char c = s.charAt(i);
            if (c == '"') {
                buff.append(c);
            }
            buff.append(c);
        }
        return buff.append('\"').toString();
    }

    /**
     * Convert an IO exception to a database exception.
     *
     * @param e       the root cause
     * @param message the message or null
     * @return the database exception object
     */
    public static TmpFileException convertIOException(int errorCode, IOException e, String message) {
        if (message == null) {
            Throwable t = e.getCause();
            if (t instanceof TmpFileException) {
                return (TmpFileException) t;
            }
            return get(errorCode, e.toString());
        }
        return get(errorCode * 10, e.toString(), message);
    }

}
