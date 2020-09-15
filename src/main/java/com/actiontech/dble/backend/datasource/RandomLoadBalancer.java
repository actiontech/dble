package com.actiontech.dble.backend.datasource;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class RandomLoadBalancer extends AbstractLoadBalancer {

    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    public RandomLoadBalancer() {
    }

    @Override
    protected PhysicalDbInstance doSelect(List<PhysicalDbInstance> okSources) {
        int length = okSources.size();
        int totalWeight = 0;
        boolean sameWeight = true;
        for (int i = 0; i < length; i++) {
            int readWeight = okSources.get(i).getConfig().getReadWeight();
            totalWeight += readWeight;
            if (sameWeight && i > 0 && readWeight != okSources.get(i - 1).getConfig().getReadWeight()) {
                sameWeight = false;
            }
        }

        if (totalWeight > 0 && !sameWeight) {
            // random by different weight
            int offset = random.nextInt(totalWeight);
            for (PhysicalDbInstance okSource : okSources) {
                offset -= okSource.getConfig().getReadWeight();
                if (offset < 0) {
                    return okSource;
                }
            }
        }
        return okSources.get(random.nextInt(length));
    }
}
