/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.config.loader.zkprocess.zookeeper;

import java.util.List;

/**
 * DirectoryInf
 * <p>
 * <p>
 * author:liujun
 * Created:2016/9/15
 */
public interface DirectoryInf {

    String getDirectoryInfo();

    void add(DirectoryInf directory);

    void add(DataInf data);

    List<Object> getSubordinateInfo();

    String getDataName();

}
