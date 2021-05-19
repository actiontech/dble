package com.actiontech.dble.net.service;

import javax.annotation.Nonnull;
import java.util.Arrays;

/**
 * Created by szf on 2020/6/18.
 */
public class NormalServiceTask extends ServiceTask {
    private final byte[] orgData;
    private final boolean reuse;
    private int extraPartOfBigPacketCount = 0;

    /**
     * @param orgData
     * @param service
     * @param extraPartsOfBigPacketCount if orgData are big packet, it contains some *extra* parts of big packet,you should pass the count.If orgData isn't big packet ,just set 0.
     */
    public NormalServiceTask(byte[] orgData, Service service, int extraPartsOfBigPacketCount) {
        super(service);
        this.orgData = orgData;
        this.reuse = false;
        this.extraPartOfBigPacketCount = extraPartsOfBigPacketCount;
    }

    public NormalServiceTask(byte[] orgData, Service service, boolean reuse) {
        super(service);
        this.orgData = orgData;
        this.reuse = reuse;
        this.extraPartOfBigPacketCount = 0;
    }


    @Nonnull
    @Override
    public ServiceTaskType getType() {
        return ServiceTaskType.NORMAL;
    }

    public byte[] getOrgData() {
        return orgData;
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

    @Override
    public String toString() {
        return "NormalServiceTask{" +
                "orgData=" + Arrays.toString(orgData) +
                ", reuse=" + reuse +
                ", extraPartOfBigPacketCount=" + extraPartOfBigPacketCount +
                ", " + super.toString() +
                '}';
    }
}
