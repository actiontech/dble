/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.cluster.general.listener;

import com.oceanbase.obsharding_d.cluster.general.bean.SubscribeReturnBean;
import com.oceanbase.obsharding_d.cluster.values.AnyType;
import com.oceanbase.obsharding_d.cluster.values.ClusterValue;
import com.oceanbase.obsharding_d.cluster.values.OriginChangeType;
import com.oceanbase.obsharding_d.cluster.values.OriginClusterEvent;

import java.util.*;

/**
 * @author dcy
 * Create Date: 2021-05-21
 */
public class UcoreListenerHelper {
    private Map<String, String> cache = new HashMap<>();

    public Collection<OriginClusterEvent<?>> getDiffList(SubscribeReturnBean output) {
        Collection<OriginClusterEvent<?>> diffList = new ArrayList<>();
        Map<String, String> newKeyMap = new HashMap<>();

        //find out the new key & changed key
        for (int i = 0; i < output.getKeysCount(); i++) {
            final String newValue = output.getValues(i);
            final ClusterValue<AnyType> newClusterValue = ClusterValue.readFromJson(newValue, AnyType.class);
            final String path = output.getKeys(i);
            newKeyMap.put(path, newValue);
            if (cache.get(path) != null) {
                final String oldValue = cache.get(path);
                if (!Objects.equals(newValue, oldValue)) {
                    //update
                    final ClusterValue<AnyType> oldClusterValue = ClusterValue.readFromJson(oldValue, AnyType.class);
                    diffList.add(new OriginClusterEvent<>(path, newClusterValue, oldClusterValue, OriginChangeType.UPDATE));
                }
            } else {
                //add
                diffList.add(new OriginClusterEvent<>(path, newClusterValue, OriginChangeType.ADDED));
            }
        }

        //find out the deleted Key
        for (Map.Entry<String, String> entry : cache.entrySet()) {
            //remove
            if (!newKeyMap.containsKey(entry.getKey())) {
                diffList.add(new OriginClusterEvent<>(entry.getKey(), ClusterValue.readFromJson(entry.getValue(), AnyType.class), OriginChangeType.REMOVED));
            }
        }

        cache = newKeyMap;

        return diffList;
    }


    public Collection<OriginClusterEvent<?>> onFirst(SubscribeReturnBean output) {
        Collection<OriginClusterEvent<?>> diffList = new ArrayList<>();
        for (int i = 0; i < output.getKeysCount(); i++) {
            final ClusterValue<AnyType> clusterValue = ClusterValue.readFromJson(output.getValues(i), AnyType.class);
            final String path = output.getKeys(i);
            diffList.add(new OriginClusterEvent<>(path, clusterValue, OriginChangeType.ADDED));
            cache.put(path, output.getValues(i));
        }
        return diffList;
    }

    public static Comparator<OriginClusterEvent<?>> sortRule() {
        return Comparator.<OriginClusterEvent<?>>comparingLong((ele) -> ele.getValue().getCreatedAt());
    }


}
