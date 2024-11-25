/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql;

import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.oceanbase.obsharding_d.net.mysql.BinaryPacket;
import com.oceanbase.obsharding_d.net.service.ResultFlag;
import com.oceanbase.obsharding_d.net.service.WriteFlags;
import com.oceanbase.obsharding_d.route.RouteResultsetNode;
import com.oceanbase.obsharding_d.services.mysqlsharding.MySQLResponseService;
import com.oceanbase.obsharding_d.singleton.FlowController;
import com.oceanbase.obsharding_d.sqlengine.mpp.LoadData;

import java.io.*;
import java.util.List;

/**
 * Created by nange on 2015/3/31.
 */
public final class LoadDataUtil {

    private LoadDataUtil() {
    }

    public static void requestFileDataResponse(byte[] data, MySQLResponseService service) {
        byte packId = data[3];
        RouteResultsetNode rrn = (RouteResultsetNode) service.getAttachment();
        LoadData loadData = rrn.getLoadData();
        List<String> loadDataData = loadData.getData();
        service.setExecuting(false);
        BufferedInputStream in = null;
        try {
            if (loadDataData != null && loadDataData.size() > 0) {
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                for (String loadDataDataLine : loadDataData) {
                    String s = loadDataDataLine + loadData.getLineTerminatedBy();
                    byte[] bytes = s.getBytes(CharsetUtil.getJavaCharset(loadData.getCharset()));
                    bos.write(bytes);
                }
                packId = writeToBackConnection(packId, new ByteArrayInputStream(bos.toByteArray()), service);
            } else {
                in = new BufferedInputStream(new FileInputStream(loadData.getFileName()));
                packId = writeToBackConnection(packId, in, service);
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
            service.write(empty, WriteFlags.QUERY_END, ResultFlag.OTHER);
        }
    }

    public static byte writeToBackConnection(byte packID, InputStream inputStream, MySQLResponseService service) throws IOException {
        try {
            int packSize = SystemConfig.getInstance().getBufferPoolChunkSize() - 5;
            // int packSize = c.getMaxPacketSize() / 32;
            //  int packSize=65530;
            byte[] buffer = new byte[packSize];
            int len = -1;

            while ((len = inputStream.read(buffer)) != -1) {
                if (FlowController.isEnableFlowControl()) {
                    FlowController.checkFlowControl(service);
                }
                byte[] temp = null;
                if (len == packSize) {
                    temp = buffer;
                } else {
                    temp = new byte[len];
                    System.arraycopy(buffer, 0, temp, 0, len);
                }
                service.getConnection().updateLastReadTime();
                BinaryPacket packet = new BinaryPacket();
                packet.setPacketId(++packID);
                packet.setData(temp);
                packet.write(service);
            }
        } finally {
            inputStream.close();
        }
        return packID;
    }
}
