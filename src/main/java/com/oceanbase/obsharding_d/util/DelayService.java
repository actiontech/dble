/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.util;

import com.oceanbase.obsharding_d.btrace.provider.GeneralProvider;
import com.oceanbase.obsharding_d.util.exception.DirectPrintException;
import com.oceanbase.obsharding_d.util.exception.NeedDelayedException;

import java.util.function.Supplier;

/**
 * @author dcy
 * Create Date: 2021-09-01
 */
public class DelayService {
    private volatile boolean doing = false;
    private final Supplier<Boolean> delayCondition;
    private final Supplier<String> checkOperationAvailableCondition;

    public DelayService(Supplier<Boolean> delayCondition, Supplier<String> checkOperationAvailableCondition) {
        this.delayCondition = delayCondition;
        this.checkOperationAvailableCondition = checkOperationAvailableCondition;
    }

    public boolean isNeedDelay() {
        return delayCondition.get();
    }

    public boolean isDoing() {
        return doing;
    }

    public void markDone() {
        this.doing = false;
    }


    public void markDoingOrDelay(boolean checkOperationAvailable) throws NeedDelayedException, DirectPrintException {
        /*
        'doing' must be set before 'delayCondition'
         */
        this.doing = true;
        if (delayCondition.get()) {
            throw new NeedDelayedException();
        } else {
            //if no need delay, then check operation available
            if (checkOperationAvailable) {
                final String msg = checkOperationAvailableCondition.get();
                if (msg != null) {
                    throw new DirectPrintException(msg);
                }
            }
            GeneralProvider.afterDelayServiceMarkDoing();
        }
    }
}
