package com.actiontech.dble.config.loader.ucoreprocess.listen;

import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreConfig;
import com.actiontech.dble.config.loader.ucoreprocess.UcorePathUtil;
import com.actiontech.dble.config.loader.ucoreprocess.UcoreXmlLoader;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import com.actiontech.dble.log.alarm.AlarmCode;
import com.actiontech.dble.log.alarm.UcoreGrpc;
import com.actiontech.dble.log.alarm.UcoreInterface;
import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by szf on 2018/1/24.
 */
public class UcoreClearKeyListener implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(UcoreClearKeyListener.class);

    private Map<String, UcoreXmlLoader> childService = new HashMap<>();

    private Map<String, String> cache = new HashMap<>();

    private UcoreGrpc.UcoreBlockingStub stub = null;

    private long index = 0;

    public void init() {
        Channel channel = ManagedChannelBuilder.forAddress(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_IP),
                Integer.parseInt(UcoreConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_PLUGINS_PORT))).usePlaintext(true).build();
        stub = UcoreGrpc.newBlockingStub(channel);
    }

    @Override
    public void run() {
        for (; ; ) {
            UcoreInterface.SubscribeKvPrefixInput input
                    = UcoreInterface.SubscribeKvPrefixInput.newBuilder().setIndex(index).setDuration(60).setKeyPrefix(UcorePathUtil.CONF_BASE_PATH).build();
            UcoreInterface.SubscribeKvPrefixOutput output = stub.subscribeKvPrefix(input);
            Map<String, UKvBean> diffMap = getDiffMap(output);
            if (output.getIndex() != index) {
                handle(diffMap);
                index = output.getIndex();
            }
        }
    }


    private Map<String, UKvBean> getDiffMap(UcoreInterface.SubscribeKvPrefixOutput output) {
        Map<String, UKvBean> diffMap = new HashMap<String, UKvBean>();
        Map<String, String> newKeyMap = new HashMap<String, String>();

        //find out the new key & changed key
        for (int i = 0; i < output.getKeysCount(); i++) {
            newKeyMap.put(output.getKeys(i), output.getValues(i));
            if (cache.get(output.getKeys(i)) != null) {
                if (!cache.get(output.getKeys(i)).equals(output.getValues(i))) {
                    diffMap.put(output.getKeys(i), new UKvBean(output.getKeys(i), output.getValues(i), UKvBean.UPDATE));
                }
            } else {
                diffMap.put(output.getKeys(i), new UKvBean(output.getKeys(i), output.getValues(i), UKvBean.ADD));
            }
        }

        //find out the deleted Key
        for (Map.Entry<String, String> entry : cache.entrySet()) {
            if (!newKeyMap.containsKey(entry.getKey())) {
                diffMap.put(entry.getKey(), new UKvBean(entry.getKey(), entry.getValue(), UKvBean.DELETE));
            }
        }

        cache = newKeyMap;

        return diffMap;
    }

    public void initForXml() {
        UcoreInterface.SubscribeKvPrefixInput input
                = UcoreInterface.SubscribeKvPrefixInput.newBuilder().setIndex(0).setDuration(60).setKeyPrefix(UcorePathUtil.BASE_PATH).build();
        UcoreInterface.SubscribeKvPrefixOutput output = stub.subscribeKvPrefix(input);

        Map<String, UKvBean> diffMap = new HashMap<String, UKvBean>();
        for (int i = 0; i < output.getKeysCount(); i++) {
            diffMap.put(output.getKeys(i), new UKvBean(output.getKeys(i), output.getValues(i), UKvBean.ADD));
        }
        handle(diffMap);
    }


    /**
     * handle the back data from the subscribe
     * if the config version changes,write the file
     * or just start a new waiting
     *
     */
    public void handle(Map<String, UKvBean> diffMap) {
        try {
            for (Map.Entry<String, UKvBean> entry:diffMap.entrySet()) {
                UcoreXmlLoader x = childService.get(entry.getKey());
                if (x != null) {
                    x.notifyProcess(entry.getValue());
                }
            }
        } catch (Exception e) {
            LOGGER.warn(AlarmCode.CORE_CLUSTER_WARN + " ucore data parse to xml error ");
            e.printStackTrace();
        }
    }


    /**
     * add ucoreXmlLoader into the watch list
     * every loader knows the path of it self
     *
     * @param loader
     * @param path
     */
    public void addChild(UcoreXmlLoader loader, String path) {
        this.childService.put(path, loader);
    }

    public void initAllNode() {
        for (Map.Entry<String, UcoreXmlLoader> service : childService.entrySet()) {
            try {
                service.getValue().notifyCluster();
            } catch (Exception e) {
                LOGGER.warn(AlarmCode.CORE_ZK_WARN + " UcoreClearKeyListener init all node error:", e);
            }
        }
    }

    public long getIndex() {
        return index;
    }
}
