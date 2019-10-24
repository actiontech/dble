/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.config;

/**
 * @author
 */
public abstract class Versions {

    public static final byte PROTOCOL_VERSION = 10;

    private static byte[] serverVersion = "5.6.29-dble-2.19.03.1-161ba1d05a0ecca0633ca0f463704149b16fdef7-20191024160111".getBytes();
    public static final byte[] VERSION_COMMENT = "dble Server (ActionTech)".getBytes();
    public static final String ANNOTATION_NAME = "dble:";
    public static final String ROOT_PREFIX = "dble";
    public static final String DOMAIN = "http://dble.cloud/";
    public static final String CONFIG_VERSION = "2.19.03.1";

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

    public static byte[] getServerVersion() {
        return serverVersion;
    }
}
