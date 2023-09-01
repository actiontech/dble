package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.config.ErrorCode;
import com.actiontech.dble.services.manager.ManagerService;
import com.actiontech.dble.singleton.ThreadManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class ThreadHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadHandler.class);
    private static final Pattern THREAD_KILL = Pattern.compile("^\\s*@@kill\\s*(name|poolname)\\s*=\\s*'([a-zA-Z_0-9\\-]+)'?$", Pattern.CASE_INSENSITIVE);
    private static final Pattern THREAD_RECOVER = Pattern.compile("^\\s*@@recover\\s*(name|poolname)\\s*=\\s*'([a-zA-Z_0-9\\-]+)'?$", Pattern.CASE_INSENSITIVE);

    private ThreadHandler() {
    }

    public static void handle(String stmt, ManagerService service, int offset) {
        String sql = stmt.substring(offset).trim();
        Matcher kill = THREAD_KILL.matcher(sql);
        Matcher recover = THREAD_RECOVER.matcher(sql);
        try {
            if (kill.matches()) {
                String type = kill.group(1);
                String name = kill.group(2);
                kill(service, type, name);
            } else if (recover.matches()) {
                String type = recover.group(1);
                String name = recover.group(2);
                recover(service, type, name);
            } else {
                service.writeErrMessage(ErrorCode.ER_YES, "Syntax Error, Please check the help to use the thread command");
            }
        } catch (Exception e) {
            LOGGER.info("thread command happen exception:", e);
            service.writeErrMessage(ErrorCode.ER_YES, e.getMessage());
        }
    }

    public static void kill(ManagerService service, String type, String name) throws Exception {
        if (type.equalsIgnoreCase("name")) {
            ThreadManager.interruptSingleThread(name);
        } else if (type.equalsIgnoreCase("poolname")) {
            ThreadManager.shutDownThreadPool(name);
        }
        service.writeOkPacket("Please see logs in logs/thread.log");
    }

    public static void recover(ManagerService service, String type, String name) throws Exception {
        if (type.equalsIgnoreCase("name")) {
            ThreadManager.recoverSingleThread(name);
        } else if (type.equalsIgnoreCase("poolname")) {
            ThreadManager.recoverThreadPool(name);
        }
        service.writeOkPacket("Please see logs in logs/thread.log");
    }
}
