/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.xa;

/**
 * Created by zhangchao on 2016/10/17.
 */
public final class Serializer {
    private Serializer() {
    }

    private static final String PROPERTY_SEPARATOR = ",";
    private static final String QUOTE = "\"";
    private static final String END_ARRAY = "]";
    private static final String START_ARRAY = "[";
    private static final String START_OBJECT = "{";
    private static final String END_OBJECT = "}";
    public static final String LINE_SEPARATOR = System.getProperty("line.separator");

    public static String toJson(CoordinatorLogEntry coordinatorLogEntry) {
        StringBuilder strBuilder = new StringBuilder(600);
        strBuilder.append(START_OBJECT);
        strBuilder.append(QUOTE).append(CoordinatorLogEntry.ID).append(QUOTE).append(":").append(QUOTE).append(coordinatorLogEntry.getId()).append(QUOTE);
        strBuilder.append(PROPERTY_SEPARATOR);
        strBuilder.append(QUOTE).append(CoordinatorLogEntry.STATE).append(QUOTE).append(":").append(QUOTE).append(coordinatorLogEntry.getTxState()).append(QUOTE);
        strBuilder.append(PROPERTY_SEPARATOR);

        String prefix = "";
        if (coordinatorLogEntry.getParticipants().length > 0) {
            strBuilder.append(QUOTE).append(CoordinatorLogEntry.PARTICIPANTS).append(QUOTE);
            strBuilder.append(":");
            strBuilder.append(START_ARRAY);

            for (ParticipantLogEntry participantLogEntry : coordinatorLogEntry.getParticipants()) {
                if (participantLogEntry == null) {
                    continue;
                }
                strBuilder.append(prefix);
                prefix = PROPERTY_SEPARATOR;
                strBuilder.append(START_OBJECT);
                strBuilder.append(QUOTE).append(CoordinatorLogEntry.P_HOST).append(QUOTE).append(":").append(QUOTE).append(participantLogEntry.getHost()).append(QUOTE);
                strBuilder.append(PROPERTY_SEPARATOR);
                strBuilder.append(QUOTE).append(CoordinatorLogEntry.P_PORT).append(QUOTE).append(":").append(QUOTE).append(participantLogEntry.getPort()).append(QUOTE);
                strBuilder.append(PROPERTY_SEPARATOR);
                strBuilder.append(QUOTE).append(CoordinatorLogEntry.P_STATE).append(QUOTE).append(":").append(QUOTE).append(participantLogEntry.getTxState()).append(QUOTE);
                strBuilder.append(PROPERTY_SEPARATOR);
                strBuilder.append(QUOTE).append(CoordinatorLogEntry.P_EXPIRES).append(QUOTE).append(":").append(participantLogEntry.getExpires());
                if (participantLogEntry.getSchema() != null) {
                    strBuilder.append(PROPERTY_SEPARATOR);
                    strBuilder.append(QUOTE).append(CoordinatorLogEntry.P_SCHEMA).append(QUOTE).append(":").append(QUOTE).append(participantLogEntry.getSchema()).append(QUOTE);
                }
                strBuilder.append(END_OBJECT);
            }
            strBuilder.append(END_ARRAY);
        }
        strBuilder.append(END_OBJECT);
        strBuilder.append(LINE_SEPARATOR);
        return strBuilder.toString();
    }
}
