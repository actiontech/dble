/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.manager.response;

import com.actiontech.dble.cluster.values.ConfStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ReloadContext {
    private final List<UniqueDbInstance> affectDbInstanceList = new ArrayList<>();

    private ConfStatus.Status confStatus = null;


    public ConfStatus.Status getConfStatus() {
        return confStatus;
    }

    public void setConfStatus(ConfStatus.Status confStatus) {
        this.confStatus = confStatus;
    }

    public List<UniqueDbInstance> getAffectDbInstanceList() {
        return Collections.unmodifiableList(affectDbInstanceList);
    }

    public void addAffectDbInstance(UniqueDbInstance dbInstance) {
        affectDbInstanceList.add(dbInstance);
    }
}
