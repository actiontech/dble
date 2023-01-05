/*
 * Copyright (C) 2016-2023 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.services.rwsplit;

import com.actiontech.dble.net.service.AbstractService;

import javax.annotation.Nonnull;

public interface StatisticsHandler {

    void stringEof(byte[] data, @Nonnull AbstractService service);
}
