package com.actiontech.dble.services.manager.handler;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.services.manager.response.FreshBackendConn;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class FreshBackendConnHandler {
    private static final String DB_NAME_FORMAT = "a-zA-Z_0-9\\-\\."; //nmtoken
    // fresh conn [forced] dbGroup ='groupName'[dbInstance ='instanceName']
    private static final Pattern FRESH_CONN = Pattern.compile("\\s*where\\s*dbGroup\\s*=\\s*'([" + DB_NAME_FORMAT + "]+)'" +
            "\\s*(and\\s*dbInstance\\s*=\\s*'([" + DB_NAME_FORMAT + "\\,]+)')?$", Pattern.CASE_INSENSITIVE);
    private static final Pattern FRESH_CONN_FORCED = Pattern.compile("\\s*forced\\s*where\\s*dbGroup\\s*=\\s*'([" + DB_NAME_FORMAT + "]+)'" +
            "\\s*(and\\s*dbInstance\\s*=\\s*'([" + DB_NAME_FORMAT + "\\,]+)')?$", Pattern.CASE_INSENSITIVE);

    private FreshBackendConnHandler() {
    }

    public static void handle(String stmt, ManagerService service, int offset) {
        String options = stmt.substring(offset).trim();
        Matcher freshConn = FRESH_CONN.matcher(options);
        Matcher freshConnForced = FRESH_CONN_FORCED.matcher(options);
        if (freshConn.matches()) {
            FreshBackendConn.execute(service, freshConn, false);
        } else if (freshConnForced.matches()) {
            FreshBackendConn.execute(service, freshConnForced, true);
        } else {
            service.writeErrMessage(ErrorCode.ER_YES, "Syntax Error,Please check the help to use the fresh conn command");
        }
    }
}
