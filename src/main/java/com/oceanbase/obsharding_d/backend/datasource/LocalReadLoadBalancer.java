/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.datasource;

import com.oceanbase.obsharding_d.config.model.SystemConfig;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class LocalReadLoadBalancer extends AbstractLoadBalancer {

    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    public LocalReadLoadBalancer() {
    }

    @Override
    protected PhysicalDbInstance doSelect(List<PhysicalDbInstance> okSources) {
        List<PhysicalDbInstance> matchSources = matchSources(okSources);
        if (matchSources.isEmpty()) {
            matchSources = okSources;
        }
        int length = matchSources.size();
        int totalWeight = 0;
        boolean sameWeight = true;
        for (int i = 0; i < length; i++) {
            int readWeight = matchSources.get(i).getConfig().getReadWeight();
            totalWeight += readWeight;
            if (sameWeight && i > 0 && readWeight != matchSources.get(i - 1).getConfig().getReadWeight()) {
                sameWeight = false;
            }
        }

        if (totalWeight > 0 && !sameWeight) {
            // random by different weight
            int offset = random.nextInt(totalWeight);
            for (PhysicalDbInstance okSource : matchSources) {
                offset -= okSource.getConfig().getReadWeight();
                if (offset < 0) {
                    return okSource;
                }
            }
        }
        return matchSources.get(random.nextInt(length));
    }

    private List<PhysicalDbInstance> matchSources(List<PhysicalDbInstance> okSources) {
        String district = SystemConfig.getInstance().getDistrict();
        String dataCenter = SystemConfig.getInstance().getDataCenter();
        if (Strings.isNullOrEmpty(district)) {
            return okSources;
        }
        List<PhysicalDbInstance> firstSources = Lists.newArrayList();
        List<PhysicalDbInstance> secondSources = Lists.newArrayList();
        List<PhysicalDbInstance> otherSources;

        otherSources = okSources.stream().filter(okSource -> okSource.isAlive()).collect(Collectors.toList());

        for (PhysicalDbInstance okSource : otherSources) {
            String dbDistrict = okSource.getConfig().getDbDistrict();
            String dbDataCenter = okSource.getConfig().getDbDataCenter();
            if (StringUtils.equalsIgnoreCase(district, dbDistrict)) {
                secondSources.add(okSource);
                if (StringUtils.equalsIgnoreCase(dataCenter, dbDataCenter)) {
                    firstSources.add(okSource);
                }
            }
        }
        if (!firstSources.isEmpty()) {
            return firstSources;
        }
        if (!secondSources.isEmpty()) {
            return secondSources;
        }
        return otherSources;

    }
}
