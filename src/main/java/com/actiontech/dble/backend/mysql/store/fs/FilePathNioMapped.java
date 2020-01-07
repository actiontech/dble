/*
 * Copyright (C) 2016-2020 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.backend.mysql.store.fs;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * @author ActionTech
 * @CreateTime 2014-8-21
 */
public class FilePathNioMapped extends FilePathWrapper {

    @Override
    public FileChannel open(String mode) throws IOException {
        return new FileNioMapped(name.substring(getScheme().length() + 1), mode);
    }

    @Override
    public String getScheme() {
        return "nioMapped";
    }

}

