/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.ucoreprocess;

import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;

/**
 * Created by szf on 2018/1/26.
 */
public interface UcoreXmlLoader {

    void notifyProcess(UKvBean configValue) throws Exception;

    void notifyCluster() throws Exception;
}
