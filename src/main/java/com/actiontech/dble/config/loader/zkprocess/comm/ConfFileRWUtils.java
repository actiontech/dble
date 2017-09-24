/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.comm;

import com.actiontech.dble.config.loader.console.ZookeeperPath;
import com.actiontech.dble.util.ResourceUtil;
import com.alibaba.fastjson.util.IOUtils;

import java.io.*;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Created by huqing.yan on 2017/6/15.
 */
public final class ConfFileRWUtils {
    private ConfFileRWUtils() {
    }

    public static String readFile(String name) throws IOException {
        StringBuilder mapFileStr = new StringBuilder();
        String path = ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey() + name;
        InputStream input = ResourceUtil.getResourceAsStreamFromRoot(path);
        checkNotNull(input, "read file curr Path :" + path + " is null! It must be not null");
        byte[] buffers = new byte[256];
        try {
            int readIndex;
            while ((readIndex = input.read(buffers)) != -1) {
                mapFileStr.append(new String(buffers, 0, readIndex));
            }
        } finally {
            IOUtils.close(input);
        }
        return mapFileStr.toString();
    }

    public static void writeFile(String name, String value) throws IOException {
        String path = ResourceUtil.getResourcePathFromRoot(ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey());
        checkNotNull(path, "write ecache file curr Path :" + path + " is null! It must be not null");
        path = new File(path).getPath() + File.separator + name;

        ByteArrayInputStream input = null;
        byte[] buffers = new byte[256];
        FileOutputStream output = null;
        try {
            int readIndex;
            input = new ByteArrayInputStream(value.getBytes());
            output = new FileOutputStream(path);
            while ((readIndex = input.read(buffers)) != -1) {
                output.write(buffers, 0, readIndex);
            }
        } finally {
            IOUtils.close(output);
            IOUtils.close(input);
        }
    }
}
