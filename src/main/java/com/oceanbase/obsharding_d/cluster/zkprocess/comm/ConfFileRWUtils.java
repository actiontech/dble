/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.zkprocess.comm;

import com.oceanbase.obsharding_d.cluster.path.ClusterPathUtil;
import com.oceanbase.obsharding_d.util.ResourceUtil;

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
        String path = ClusterPathUtil.LOCAL_WRITE_PATH + name;
        InputStream input = null;
        try {
            input = ResourceUtil.getResourceAsStreamFromRoot(path);
            checkNotNull(input, "read file curr Path :" + path + " is null! It must be not null");
            byte[] buffers = new byte[256];
            int readIndex;
            while ((readIndex = input.read(buffers)) != -1) {
                mapFileStr.append(new String(buffers, 0, readIndex));
            }
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e1) {
                    // ignore error
                }
            }
        }
        return mapFileStr.toString();
    }

    public static void writeFile(String name, String value) throws IOException {
        String path = ResourceUtil.getResourcePathFromRoot(ClusterPathUtil.LOCAL_WRITE_PATH);
        checkNotNull(path, "writeDirectly ecache file curr Path :" + path + " is null! It must be not null");
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
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e1) {
                    // ignore error
                }
            }
            if (output != null) {
                try {
                    output.close();
                } catch (IOException e1) {
                    // ignore error
                }
            }
        }
    }
}
