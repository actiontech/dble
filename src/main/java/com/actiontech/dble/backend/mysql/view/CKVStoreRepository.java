package com.actiontech.dble.backend.mysql.view;

import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.*;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import com.actiontech.dble.log.alarm.AlarmCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.actiontech.dble.util.KVPathUtil.SEPARATOR;

/**
 * Created by szf on 2018/1/23.
 */
public class CKVStoreRepository implements Repository {

    private static final Logger LOGGER = LoggerFactory.getLogger(CKVStoreRepository.class);

    private Map<String, Map<String, String>> viewCreateSqlMap = new HashMap<String, Map<String, String>>();

    @Override
    public Map<String, Map<String, String>> getViewCreateSqlMap() {
        return viewCreateSqlMap;
    }


    public CKVStoreRepository() {
        init();
    }

    @Override
    public void init() {
        List<UKvBean> allList = ClusterUcoreSender.getKeyTree(UcorePathUtil.getViewPath());
        for (UKvBean bean : allList) {
            String[] key = bean.getKey().split("/");
            if (key.length == 5) {
                String[] value = key[key.length - 1].split(SCHEMA_VIEW_SPLIT);
                if (viewCreateSqlMap.get(value[0]) == null) {
                    Map<String, String> schemaMap = new HashMap<String, String>();
                    viewCreateSqlMap.put(value[0], schemaMap);
                }
                viewCreateSqlMap.get(value[0]).put(value[1], bean.getValue());
            }
        }
    }


    @Override
    public void put(String schemaName, String viewName, String createSql) {
        Map<String, String> schemaMap = viewCreateSqlMap.get(schemaName);
        if (schemaMap == null) {
            schemaMap = new HashMap<String, String>();
            viewCreateSqlMap.put(schemaName, schemaMap);
        } else {
            if (schemaMap.get(viewName) != null &&
                    createSql.equals(schemaMap.get(viewName))) {
                //create view has no change,return with nothing
                return;
            }
        }

        StringBuffer sb = new StringBuffer().append(UcorePathUtil.getViewPath()).
                append(SEPARATOR).append(schemaName).append(SCHEMA_VIEW_SPLIT).append(viewName);
        StringBuffer lsb = new StringBuffer().append(UcorePathUtil.getViewPath()).
                append(SEPARATOR).append(LOCK).append(SEPARATOR).append(schemaName).append(SCHEMA_VIEW_SPLIT).append(viewName);
        StringBuffer nsb = new StringBuffer().append(UcorePathUtil.getViewPath()).
                append(SEPARATOR).append(UPDATE).append(SEPARATOR).append(schemaName).append(SCHEMA_VIEW_SPLIT).append(viewName);
        UDistributeLock distributeLock = new UDistributeLock(lsb.toString(),
                UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID) + SCHEMA_VIEW_SPLIT + UPDATE);

        try {
            int time = 0;
            while (!distributeLock.acquire()) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                if (time++ % 10 == 0) {
                    LOGGER.info(" view meta waiting for the lock " + schemaName + " " + viewName);
                }
            }

            schemaMap.put(viewName, createSql);
            ClusterUcoreSender.sendDataToUcore(sb.toString(), createSql);
            ClusterUcoreSender.sendDataToUcore(nsb.toString(), UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID) + SCHEMA_VIEW_SPLIT + UPDATE);

            //check if the online node number is equals to the reponse number
            List<UKvBean> onlineList = ClusterUcoreSender.getKeyTree(UcorePathUtil.getOnlinePath() + SEPARATOR);
            List<UKvBean> reponseList = ClusterUcoreSender.getKeyTree(nsb.toString() + SEPARATOR);

            while (reponseList.size() < onlineList.size() - 1) {
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                onlineList = ClusterUcoreSender.getKeyTree(UcorePathUtil.getOnlinePath());
                reponseList = ClusterUcoreSender.getKeyTree(nsb.toString());
            }

            //check all the node status is success
            for (UKvBean kv : reponseList) {
                if (!kv.getValue().equals(UcorePathUtil.SUCCESS)) {
                    LOGGER.info("view mate change error on key " + kv.getKey());
                }
            }

            ClusterUcoreSender.deleteKVTree(nsb.toString() + SEPARATOR);
            distributeLock.release();

        } catch (Exception e) {
            LOGGER.warn(AlarmCode.CORE_ZK_WARN + "delete ucore node error :　" + e.getMessage());
        }


    }


    /***
     * delete ucore K/V view meta
     * try get the view meta lock & delete the view meta
     * then create a delete view node ,wait for all the online node response
     * check all the response data send the alarm if not success
     * @param schemaName
     * @param viewName
     */
    @Override
    public void delete(String schemaName, String[] viewName) {
        for (String view : viewName) {
            StringBuffer sb = new StringBuffer().append(UcorePathUtil.getViewPath()).
                    append(SEPARATOR).append(schemaName).append(SCHEMA_VIEW_SPLIT).append(view);
            StringBuffer nsb = new StringBuffer().append(UcorePathUtil.getViewPath()).
                    append(SEPARATOR).append(UPDATE).append(SEPARATOR).
                    append(schemaName).append(SCHEMA_VIEW_SPLIT).append(view);
            StringBuffer lsb = new StringBuffer().append(UcorePathUtil.getViewPath()).
                    append(SEPARATOR).append(LOCK).append(SEPARATOR).append(schemaName).append(SCHEMA_VIEW_SPLIT).append(viewName);
            UDistributeLock distributeLock = new UDistributeLock(lsb.toString(),
                    UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID) + SCHEMA_VIEW_SPLIT + DELETE);

            try {
                viewCreateSqlMap.get(schemaName).remove(view);
                int time = 0;
                while (!distributeLock.acquire()) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
                    if (time++ % 10 == 0) {
                        LOGGER.warn(" view meta waiting for the lock " + schemaName + " " + view);
                    }
                }
                ClusterUcoreSender.deleteKV(sb.toString());
                ClusterUcoreSender.sendDataToUcore(nsb.toString(), UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID) + SCHEMA_VIEW_SPLIT + DELETE);

                //check if the online node number is equals to the reponse number
                List<UKvBean> onlineList = ClusterUcoreSender.getKeyTree(UcorePathUtil.getOnlinePath());
                List<UKvBean> reponseList = ClusterUcoreSender.getKeyTree(nsb.toString());

                while (reponseList.size() < onlineList.size() - 1) {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
                    onlineList = ClusterUcoreSender.getKeyTree(UcorePathUtil.getOnlinePath());
                    reponseList = ClusterUcoreSender.getKeyTree(nsb.toString());
                }

                //check all the node status is success
                if (reponseList != null) {
                    for (UKvBean kv : reponseList) {
                        if (!kv.getValue().equals(UcorePathUtil.SUCCESS)) {
                            LOGGER.info(AlarmCode.CORE_CLUSTER_WARN + "view mate change error on key " + kv.getKey());
                        }
                    }
                }

                ClusterUcoreSender.deleteKVTree(nsb.toString() + SEPARATOR);
                distributeLock.release();

            } catch (Exception e) {
                LOGGER.warn(AlarmCode.CORE_ZK_WARN + "delete ucore node error :　" + e.getMessage());
            }
        }
    }


}
