/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.util.cmd;

import java.util.HashMap;
import java.util.Map;

/**
 * -host=192.168.1.1:8080
 * -
 *
 * @author me
 */
public final class CmdArgs {
    private static final CmdArgs CMD_ARGS = new CmdArgs();

    private Map<String, String> args;

    private CmdArgs() {
        args = new HashMap<>();
    }


    public static CmdArgs getInstance(String[] args) {
        Map<String, String> cmdArgs = CmdArgs.CMD_ARGS.args;
        for (String arg1 : args) {
            String arg = arg1.trim();
            int split = arg.indexOf('=');
            cmdArgs.put(arg.substring(1, split), arg.substring(split + 1));
        }
        return CmdArgs.CMD_ARGS;
    }

    public String getString(String name) {
        return args.get(name);
    }

    public int getInt(String name) {
        return Integer.parseInt(getString(name));
    }

    public long getLong(String name) {
        return Long.parseLong(getString(name));
    }

    public boolean getBoolean(String name) {
        return Boolean.parseBoolean(getString(name));
    }
}
