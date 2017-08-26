package io.mycat.config.loader.zkprocess.entity.schema.datahost;

import javax.xml.bind.annotation.*;
import java.util.List;

/**
 * <readHost host="" url="" password="" user=""></readHost>
 *
 *
 * author:liujun
 * Created:2016/9/15
 *
 *
 *
 *
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "readHost")
public class ReadHost extends WriteHost {

    @XmlAttribute
    protected String weight;

    public String getWeight() {
        return weight;
    }

    public void setWeight(String weight) {
        this.weight = weight;
    }

    @XmlTransient
    @Override
    public List<ReadHost> getReadHost() {
        return super.getReadHost();
    }

    @Override
    public String toString() {
        String builder = "ReadHost [weight=" +
                weight +
                "]";
        return builder;
    }

}
