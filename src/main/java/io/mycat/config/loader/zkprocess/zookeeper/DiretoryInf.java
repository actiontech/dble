package io.mycat.config.loader.zkprocess.zookeeper;

import java.util.List;

/**
 * DiretoryInf
 *
 *
 * author:liujun
 * Created:2016/9/15
 *
 *
 *
 *
 */
public interface DiretoryInf {

    String getDiretoryInfo();

    void add(DiretoryInf directory);

    void add(DataInf data);

    List<Object> getSubordinateInfo();

    String getDataName();

}
