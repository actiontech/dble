package com.actiontech.dble.cluster.general.bean;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by szf on 2019/3/12.
 */
public class SubscribeReturnBean {


    private List<KvBean> kvList = new ArrayList<>();

    private long index = 0;

    public int getKeysCount() {
        return kvList.size();
    }

    public String getKeys(int i) {
        return kvList.get(i).getKey();
    }

    public String getValues(int i) {
        return kvList.get(i).getValue();
    }

    public List<KvBean> getKvList() {
        return kvList;
    }

    public void setKvList(List<KvBean> kvList) {
        this.kvList = kvList;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
    }

}
