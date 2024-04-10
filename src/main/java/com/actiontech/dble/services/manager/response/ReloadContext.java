/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import java.util.ArrayList;
import java.util.List;

public class ReloadContext {
    List<UniqueDbInstance> affectDbInstanceList = new ArrayList<>();

    public List<UniqueDbInstance> getAffectDbInstanceList() {
        return affectDbInstanceList;
    }
}
