/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.delyDetection;

public enum DelayDetectionStatus {
    INIT(), OK(), TIMEOUT(), ERROR(), STOP();

    @Override
    public String toString() {
        return super.toString().toLowerCase();
    }

}
