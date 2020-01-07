/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.xa;

import com.actiontech.dble.backend.mysql.xa.recovery.DeserializationException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhangchao on 2016/10/17.
 */
public final class Deserializer {
    private Deserializer() {
    }

    private static final String JSON_ARRAY_END = "]";

    private static final String JSON_ARRAY_START = "[";

    private static final String OBJECT_START = "{";

    private static final String OBJECT_END = "}";

    private static List<String> tokenize(String content) {
        List<String> result = new ArrayList<>();
        int endObject = content.indexOf(OBJECT_END);
        while (endObject > 0) {
            String object = content.substring(0, endObject + 1);
            result.add(object);
            content = content.substring(endObject + 1);
            endObject = content.indexOf(OBJECT_END);
        }
        return result;
    }

    private static String extractArrayPart(String content) {
        if (!content.contains(JSON_ARRAY_START) && !content.contains(JSON_ARRAY_END)) {
            //no array...
            return "";
        }
        //else
        int start = content.indexOf(JSON_ARRAY_START);
        int end = content.indexOf(JSON_ARRAY_END);

        return content.substring(start + 1, end);
    }

    public static CoordinatorLogEntry fromJson(String coordinatorLogEntryStr) throws DeserializationException {
        try {
            String jsonContent = coordinatorLogEntryStr.trim();
            validateJsonContent(jsonContent);
            Map<String, String> header = extractHeader(jsonContent);
            String coordinatorId = header.get(CoordinatorLogEntry.ID);
            String arrayContent = extractArrayPart(jsonContent);
            List<String> elements = tokenize(arrayContent);

            ParticipantLogEntry[] participantLogEntries = new ParticipantLogEntry[elements.size()];

            for (int i = 0; i < participantLogEntries.length; i++) {
                participantLogEntries[i] = recreateParticipantLogEntry(coordinatorId, elements.get(i));
            }
            CoordinatorLogEntry actual = new CoordinatorLogEntry(coordinatorId, participantLogEntries, TxState.valueOf(Integer.parseInt(header.get(CoordinatorLogEntry.STATE))));
            return actual;
        } catch (Exception unexpectedEOF) {
            throw new DeserializationException(coordinatorLogEntryStr);
        }
    }

    private static void validateJsonContent(String coordinatorLogEntryStr)
            throws DeserializationException {
        if (!coordinatorLogEntryStr.startsWith(OBJECT_START)) {
            throw new DeserializationException(coordinatorLogEntryStr);
        }
        if (!coordinatorLogEntryStr.endsWith(OBJECT_END)) {
            throw new DeserializationException(coordinatorLogEntryStr);
        }
    }

    private static Map<String, String> extractHeader(String coordinatorLogEntryStr) {
        Map<String, String> header = new HashMap<>(2);
        String[] attributes = coordinatorLogEntryStr.split(",");
        for (String attribute : attributes) {
            String[] pair = attribute.split(":");
            header.put(pair[0].replaceAll("\\{", "").replace("\"", ""), pair[1].replace("\"", ""));
        }
        return header;
    }

    private static ParticipantLogEntry recreateParticipantLogEntry(String coordinatorId,
                                                                   String participantLogEntry) {
        participantLogEntry = participantLogEntry.replaceAll("\\{", "").replaceAll("\\}", "");

        Map<String, String> content = new HashMap<>(5);
        String[] attributes = participantLogEntry.split(",");
        for (String attribute : attributes) {
            String[] pair = attribute.split(":");
            if (pair.length > 1) {
                content.put(pair[0].replace("\"", ""), pair[1].replace("\"", ""));
            }

        }

        ParticipantLogEntry actual = new ParticipantLogEntry(coordinatorId,
                content.get(CoordinatorLogEntry.P_HOST), Integer.parseInt(content.get(CoordinatorLogEntry.P_PORT)), Long.parseLong(content.get(CoordinatorLogEntry.P_EXPIRES)), content.get(CoordinatorLogEntry.P_SCHEMA), TxState.valueOf(Integer.parseInt(content.get(CoordinatorLogEntry.P_STATE))));
        return actual;
    }

}
