package com.actiontech.dble.config.loader.ucoreprocess;

import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;

/**
 * Created by szf on 2018/1/26.
 */
public interface UcoreXmlLoader {

    void notifyProcess(UKvBean configValue) throws Exception;

    void notifyCluster() throws Exception;

    void notifyProcessWithKey(String key, String value) throws Exception;
}
