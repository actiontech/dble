/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.backend.mysql.store.fs;

import java.io.IOException;
import java.nio.channels.FileChannel;

/**
 * <pre>
 * This file system stores files on disk and uses java.nio to access the files.
 * </pre>
 *
 * @author oceanbase
 * @CreateTime 2014-8-21
 */
public class FilePathNio extends FilePathWrapper {

    @Override
    public FileChannel open(String mode) throws IOException {
        return new FileNio(name.substring(getScheme().length() + 1), mode);
    }

    @Override
    public String getScheme() {
        return "nio";
    }
}

