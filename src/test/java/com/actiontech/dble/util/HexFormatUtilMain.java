/*
* Copyright (C) 2016-2019 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.util;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mycat
 */
public class HexFormatUtilMain {

    public static void main(String[] args) {
        List<String> srcList = new ArrayList<String>();
        srcList.add("53 45 4C 45 43 54 20 4C 41 53 54 5F 49 4E 53 45 52 54 5F 49 44 28 29");
        srcList.add("4C 41 53 54 5F 49 4E 53 45 52 54 5F 49 44 28 29");
        srcList.add("64 65 66");
        srcList.add("73 65 6C 65 63 74 20 2A 20 66 72 6F 6D 20 62 72 6D 6D 73 5F 75 73 65 72 20 6C 69 6D 69 74 20 31");
        srcList.add("62 72 6D 6D 73 31");
        srcList.add("62 72 6D 6D 73 5F 75 73 65 72");
        srcList.add("69 64");
        srcList.add("49 4E 53 45 52 54 20 49 4E 54 4F 20 62 72 6D 6D 73 5F 75 73 65 72 20 56 41 4C 55 45 53 20 28 6E 75 6C 6C 2C 27 68 65 78 69 61 6E 6D 61 6F 27 2C 30 2C 30 2C 30 2C 30 2C 27 32 30 30 39 2D 30 33 2D 30 35 27 2C 27 31 32 31 2E 33 34 2E 31 37 38 2E 33 35 27 2C 27 32 30 30 39 2D 30 33 2D 30 35 20 31 34 3A 33 38 3A 33 35 27 2C 27 32 30 30 39 2D 30 33 2D 30 35 20 31 34 3A 33 38 3A 33 35 27 2C 27 32 30 30 39 2D 30 33 2D 30 35 20 31 34 3A 33 38 3A 33 35 27 29");
        srcList.add("73 65 6C 65 63 74 20 69 64 20 66 72 6F 6D 20 6F 66 66 65 72 20 6C 69 6D 69 74 20 3F");
        srcList.add("73 65 6C 65 63 74 20 69 64 20 66 72 6F 6D 20 6F 66 66 65 72 20 6C 69 6D 69 74 20 31");
        srcList.add("64 65 66");
        srcList.add("3F");
        srcList.add("6F 66 66 65 72 31");
        srcList.add("6F 66 66 65 72");
        srcList.add("32 39 30 34 33");
        for (int i = 0; i < srcList.size(); i++) {
            System.out.println(HexFormatUtil.fromHex(srcList.get(i), "UTF-8"));
        }
        System.out.println(HexFormatUtil.fromHex8B("73 71 00 00 00 00 00 00"));
    }

}