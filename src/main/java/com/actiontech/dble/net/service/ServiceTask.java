package com.actiontech.dble.net.service;

import java.util.Arrays;

/**
 * Created by szf on 2020/6/18.
 */
public class ServiceTask {

    private final byte[] orgData;
    private final boolean reuse;
    private final Service service;
    private int extraPartOfBigPacketCount = 0;

    /**
     * @param orgData
     * @param service
     * @param extraPartsOfBigPacketCount if orgData are big packet, it contains some *extra* parts of big packet,you should pass the count.If orgData isn't big packet ,just set 0.
     */
    public ServiceTask(byte[] orgData, Service service, int extraPartsOfBigPacketCount) {
        this.orgData = orgData;
        this.service = service;
        this.reuse = false;
        this.extraPartOfBigPacketCount = extraPartsOfBigPacketCount;
    }

    public ServiceTask(byte[] orgData, Service service, boolean reuse) {
        this.orgData = orgData;
        this.service = service;
        this.reuse = reuse;
    }

    public byte[] getOrgData() {
        return orgData;
    }

    public Service getService() {
        return service;
    }

    public boolean isReuse() {
        return reuse;
    }

    public int getLastSequenceId() {
        if (orgData == null || orgData.length < 4) {
            throw new IllegalStateException("can't get Sequence Id from " + Arrays.toString(orgData));
        }
        return (orgData[3]) + extraPartOfBigPacketCount;
    }
}
