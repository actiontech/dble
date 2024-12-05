/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.oceanbase.obsharding_d.util;

import org.apache.logging.log4j.core.util.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;

public final class IOUtil {
    private IOUtil() {
    }

    public static String convertStreamToString(InputStream is, Charset ecoding) throws IOException {
        try {
            InputStreamReader reader = new InputStreamReader(is, ecoding);
            return IOUtils.toString(reader);
        } finally {
            is.close();
        }
    }
}
