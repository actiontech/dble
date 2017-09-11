/*
* Copyright (C) 2016-2017 ActionTech.
* based on code by MyCATCopyrightHolder Copyright (c) 2013, OpenCloudDB/MyCAT.
* License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
*/
package com.actiontech.dble.model;

/**
 * @author mycat
 */
public class TransferObject {
    long handleCount;
    long compeleteCount;

    public void handle() {
        handleCount++;
    }

    public void compelete() {
        compeleteCount++;
    }

}