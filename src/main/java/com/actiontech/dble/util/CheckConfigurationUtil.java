package com.actiontech.dble.util;

import com.google.common.base.Strings;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Optional;

/**
 * special Util
 *
 * @author lin
 */
public final class CheckConfigurationUtil {
    private CheckConfigurationUtil() {
    }

    public static void checkConfiguration() throws IOException {
        String path = getHomePathSuffix();
        Optional.ofNullable(path).orElseThrow(() -> {
            System.out.println("homePath is not set");
            return new NullPointerException("homePath is not set");
        });
        if (Strings.isNullOrEmpty(trimSingleQuotes(path))) {
            System.out.println("homePath is not be null");
            throw new NullPointerException("homePath is not be null");
        }
    }

    /**
     * Returns a string whose value is this string, with any leading and trailing
     * whitespace removed.
     * note: like str = aabbccbbaa, c = b, result is cc. not bbccbb.
     *
     * @param str
     * @return
     */
    public static String trimSingleQuotes(String str) {
        char c = '\'';
        char[] value = str.toCharArray();
        int len = value.length;
        int startIndex = 0;
        char[] val = value;
        while ((startIndex < len) && (val[startIndex] <= c)) {
            startIndex++;
        }
        while ((startIndex < len) && (val[len - 1] <= c)) {
            len--;
        }
        return ((startIndex > 0) || (len < value.length)) ? str.substring(startIndex, len) : str;
    }

    private static String getHomePathSuffix() throws IOException {
        BufferedReader in = null;
        String path = null;
        String bootFilePath = "/bootstrap.cnf";
        try {
            InputStream configIS = ResourceUtil.getResourceAsStream(bootFilePath);
            Optional.ofNullable(configIS).orElseThrow(() -> {
                String msg = bootFilePath + " is not exists";
                return new IOException(msg);
            });
            in = new BufferedReader(new InputStreamReader(configIS));
            String pathPrefix = "-DhomePath=";
            for (String line; (line = in.readLine()) != null; ) {
                if (line.startsWith(pathPrefix)) {
                    path = line.substring(pathPrefix.length());
                    return path;
                }
            }
        } catch (IOException e) {
            throw e;
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e2) {
                //ignore error
            }
        }
        return path;
    }

}
