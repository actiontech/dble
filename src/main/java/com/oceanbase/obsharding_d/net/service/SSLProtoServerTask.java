/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.net.service;

import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;

public class SSLProtoServerTask extends ServiceTask {

    @Nonnull
    private final byte[] orgData;

    public SSLProtoServerTask(@Nonnull byte[] orgData, Service service) {
        super(service);
        this.orgData = orgData;
    }

    @NotNull
    @Override
    public ServiceTaskType getType() {
        return ServiceTaskType.SSL;
    }

    @Nonnull
    public byte[] getOrgData() {
        return orgData;
    }
}
