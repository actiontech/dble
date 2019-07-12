/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.backend.mysql.nio.MySQLConnection;
import com.actiontech.dble.net.mysql.BinaryPacket;
import com.actiontech.dble.route.RouteResultsetNode;
import com.actiontech.dble.sqlengine.mpp.LoadData;

import java.io.*;
import java.util.List;

/**
 * Created by nange on 2015/3/31.
 */
public final class LoadDataUtil {
    private LoadDataUtil() {
    }

    public static void requestFileDataResponse(byte[] data, BackendConnection conn) {
        byte packId = data[3];
        MySQLConnection c = (MySQLConnection) conn;
        RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
        LoadData loadData = rrn.getLoadData();
        List<String> loadDataData = loadData.getData();

        BufferedInputStream in = null;
        try {
            if (loadDataData != null && loadDataData.size() > 0) {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                for (String loadDataDataLine : loadDataData) {
                    String s = loadDataDataLine + loadData.getLineTerminatedBy();
                    byte[] bytes = s.getBytes(CharsetUtil.getJavaCharset(loadData.getCharset()));
                    bos.write(bytes);
                }
                packId = writeToBackConnection(packId, new ByteArrayInputStream(bos.toByteArray()), c);
            } else {
                in = new BufferedInputStream(new FileInputStream(loadData.getFileName()));
                packId = writeToBackConnection(packId, in, c);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (IOException e) {
                // ignore error
            }

            //send empty packet
            byte[] empty = new byte[]{0, 0, 0, 3};
            empty[3] = ++packId;
            c.write(empty);
        }
    }

    public static byte writeToBackConnection(byte packID, InputStream inputStream, MySQLConnection c) throws IOException {
        try {
            int packSize = DbleServer.getInstance().getConfig().getSystem().getBufferPoolChunkSize() - 5;
            // int packSize = c.getMaxPacketSize() / 32;
            //  int packSize=65530;
            byte[] buffer = new byte[packSize];
            int len = -1;

            while ((len = inputStream.read(buffer)) != -1) {
                byte[] temp = null;
                if (len == packSize) {
                    temp = buffer;
                } else {
                    temp = new byte[len];
                    System.arraycopy(buffer, 0, temp, 0, len);
                }
                BinaryPacket packet = new BinaryPacket();
                packet.setPacketId(++packID);
                packet.setData(temp);
                packet.write(c);
            }
        } finally {
            inputStream.close();
        }
        return packID;
    }
}
