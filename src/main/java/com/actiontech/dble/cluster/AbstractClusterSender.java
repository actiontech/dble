package com.actiontech.dble.cluster;

import com.actiontech.dble.cluster.bean.KvBean;
import com.actiontech.dble.cluster.kVtoXml.ClusterToXml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

/**
 * Created by szf on 2019/3/11.
 */
public abstract class AbstractClusterSender implements ClusterSender {
    public static final Logger LOGGER = LoggerFactory.getLogger(AbstractClusterSender.class);


    public String waitingForAllTheNode(String checkString, String path) {
        Map<String, String> expectedMap = ClusterToXml.getOnlineMap();
        StringBuffer errorMsg = new StringBuffer();
        for (; ; ) {
            errorMsg.setLength(0);
            if (checkResponseForOneTime(checkString, path, expectedMap, errorMsg)) {
                break;
            }
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
        }
        return errorMsg.length() <= 0 ? null : errorMsg.toString();
    }

    public boolean checkResponseForOneTime(String checkString, String path, Map<String, String> expectedMap, StringBuffer errorMsg) {
        Map<String, String> currentMap = ClusterToXml.getOnlineMap();
        checkOnline(expectedMap, currentMap);
        List<KvBean> responseList = ClusterHelper.getKVPath(path);
        boolean flag = false;
        for (Map.Entry<String, String> entry : expectedMap.entrySet()) {
            flag = false;
            for (KvBean kvBean : responseList) {
                String responseNode = last(kvBean.getKey().split("/"));
                if (last(entry.getKey().split("/")).
                        equals(responseNode)) {
                    if (checkString != null) {
                        if (!checkString.equals(kvBean.getValue())) {
                            if (errorMsg != null) {
                                errorMsg.append(responseNode).append(":").append(kvBean.getValue()).append(";");
                            }
                        }
                    }
                    flag = true;
                    break;
                }
            }
            if (!flag) {
                break;
            }
        }

        return flag;
    }


    public void checkOnline(Map<String, String> expectedMap, Map<String, String> currentMap) {
        Iterator<Map.Entry<String, String>> iterator = expectedMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            if (!currentMap.containsKey(entry.getKey()) ||
                    (currentMap.containsKey(entry.getKey()) && !currentMap.get(entry.getKey()).equals(entry.getValue()))) {
                iterator.remove();
            }
        }

        for (Map.Entry<String, String> entry : currentMap.entrySet()) {
            if (!expectedMap.containsKey(entry.getKey())) {
                LOGGER.warn("NODE " + entry.getKey() + " IS NOT EXPECTED TO BE ONLINE,PLEASE CHECK IT ");
            }
        }
    }

    public static <T> T last(T[] array) {
        return array[array.length - 1];
    }

}
