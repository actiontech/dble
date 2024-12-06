/*
 * Copyright (C) 2016-2023 ActionTech.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.net.ssl;

import javax.net.ssl.SSLEngine;

/**
 * @author dcy
 * Create Date: 2024-12-05
 */
public interface IOpenSSLWrapper {
    public SSLEngine createClientSSLEngine();

    public boolean initContext();

    public SSLEngine createServerSSLEngine(boolean isAuthClient);
}
