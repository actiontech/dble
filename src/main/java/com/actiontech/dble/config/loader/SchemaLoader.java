/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config.loader;

import com.actiontech.dble.config.model.DataHostConfig;
import com.actiontech.dble.config.model.DataNodeConfig;
import com.actiontech.dble.config.model.ERTable;
import com.actiontech.dble.config.model.SchemaConfig;

import java.util.Map;
import java.util.Set;

/**
 * @author mycat
 */
public interface SchemaLoader {

    Map<String, DataHostConfig> getDataHosts();

    Map<String, DataNodeConfig> getDataNodes();

    Map<String, SchemaConfig> getSchemas();

    Map<ERTable, Set<ERTable>> getErRelations();
}
