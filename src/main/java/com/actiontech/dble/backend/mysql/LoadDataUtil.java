/*
 * Copyright (C) 2016-2017 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql;

import com.actiontech.dble.DbleServer;
import com.actiontech.dble.backend.BackendConnection;
import com.actiontech.dble.net.BackendAIOConnection;
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
        BackendAIOConnection backendAIOConnection = (BackendAIOConnection) conn;
        RouteResultsetNode rrn = (RouteResultsetNode) conn.getAttachment();
        LoadData loadData = rrn.getLoadData();
        List<String> loadDataData = loadData.getData();
        try {
            if (loadDataData != null && loadDataData.size() > 0) {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                for (int i = 0, loadDataDataSize = loadDataData.size(); i < loadDataDataSize; i++) {
                    String line = loadDataData.get(i);


                    String s = (i == loadDataDataSize - 1) ? line : line + loadData.getLineTerminatedBy();
                    byte[] bytes = s.getBytes(CharsetUtil.getJavaCharset(loadData.getCharset()));
                    bos.write(bytes);


                }

                packId = writeToBackConnection(packId, new ByteArrayInputStream(bos.toByteArray()), backendAIOConnection);

            } else {
                packId = writeToBackConnection(packId, new BufferedInputStream(new FileInputStream(loadData.getFileName())), backendAIOConnection);

            }
        } catch (IOException e) {

            throw new RuntimeException(e);
        } finally {
            //send empty packet
            byte[] empty = new byte[]{0, 0, 0, 3};
            empty[3] = ++packId;
            backendAIOConnection.write(empty);
        }


    }

    public static byte writeToBackConnection(byte packID, InputStream inputStream, BackendAIOConnection backendAIOConnection) throws IOException {
        try {
            int packSize = DbleServer.getInstance().getConfig().getSystem().getBufferPoolChunkSize() - 5;
            // int packSize = backendAIOConnection.getMaxPacketSize() / 32;
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
                packet.write(backendAIOConnection);
            }

        } finally {
            inputStream.close();
        }


        return packID;
    }
}
