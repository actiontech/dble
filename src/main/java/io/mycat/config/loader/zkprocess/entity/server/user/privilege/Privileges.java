package io.mycat.config.loader.zkprocess.entity.server.user.privilege;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlType;
import java.util.List;

/**
 * Created by huqing.yan on 2017/6/16.
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "privileges")
public class Privileges {
	@XmlAttribute(required = true)
	protected Boolean check;
	protected List<PriSchema> schema;
	public Boolean getCheck() {
		return check;
	}

	public void setCheck(Boolean check) {
		this.check = check;
	}

	public List<PriSchema> getSchema() {
		return schema;
	}

	public void setSchema(List<PriSchema> schema) {
		this.schema = schema;
	}
	@Override
	public String toString() {
		return "privileges{" + "check='" + check + '\'' + ", schema='" + schema + '}';
	}
}
