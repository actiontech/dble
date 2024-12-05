/*
 * Copyright (C) 2016-2023 OBsharding_D.
 * based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */
package com.oceanbase.obsharding_d.config;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * @author
 */
public abstract class Versions {

    public static final byte PROTOCOL_VERSION = 10;

    private static byte[] serverVersion = "5.6.29-OBsharding-D-9.9.9.9-f5928d276537752e7a4760aac6193f5e48531c36-20220524130848".getBytes();
    public static final byte[] VERSION_COMMENT = "OBsharding-D Server (oceanbase)".getBytes();
    public static final String ANNOTATION_NAME = "OBsharding-D:";
    public static final String ROOT_PREFIX = "OBsharding-D";
    public static final String DOMAIN = "http://OBsharding-D.cloud/";
    public static final String CONFIG_VERSION = "4.0";
    private static final Pattern VERSION_PATTERN = Pattern.compile("^(\\d{1,})\\.(\\d{1,})$", Pattern.CASE_INSENSITIVE);

    public static void setServerVersion(String version) {
        byte[] mysqlVersionPart = version.getBytes();
        int startIndex;
        for (startIndex = 0; startIndex < serverVersion.length; startIndex++) {
            if (serverVersion[startIndex] == '-')
                break;
        }

        // concat version
        byte[] newVersion = new byte[mysqlVersionPart.length + serverVersion.length - startIndex];
        System.arraycopy(mysqlVersionPart, 0, newVersion, 0, mysqlVersionPart.length);
        System.arraycopy(serverVersion, startIndex, newVersion, mysqlVersionPart.length,
                serverVersion.length - startIndex);
        serverVersion = newVersion;
    }

    public static boolean checkVersion(String version) {
        Matcher matcher = VERSION_PATTERN.matcher(version);
        if (matcher.find()) {
            Matcher vmatcher = VERSION_PATTERN.matcher(CONFIG_VERSION);
            vmatcher.find();
            if (matcher.group(1).equals(vmatcher.group(1))) {
                return true;
            }
        }
        return false;
    }

    public static byte[] getServerVersion() {
        return serverVersion;
    }
}
