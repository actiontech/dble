/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.values;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/**
 * @author dcy
 * Create Date: 2021-04-02
 */
public interface SelfSerialize<T> {
    JsonObject serialize();

    T deserialize(JsonElement object);
}
