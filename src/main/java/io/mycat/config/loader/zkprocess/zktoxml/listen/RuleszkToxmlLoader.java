package io.mycat.config.loader.zkprocess.zktoxml.listen;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.config.loader.console.ZookeeperPath;
import io.mycat.config.loader.zkprocess.comm.ConfFileRWUtils;
import io.mycat.config.loader.zkprocess.comm.NotifyService;
import io.mycat.config.loader.zkprocess.comm.ZookeeperProcessListen;
import io.mycat.config.loader.zkprocess.console.ParseParamEnum;
import io.mycat.config.loader.zkprocess.entity.Property;
import io.mycat.config.loader.zkprocess.entity.Rules;
import io.mycat.config.loader.zkprocess.entity.rule.function.Function;
import io.mycat.config.loader.zkprocess.entity.rule.tablerule.TableRule;
import io.mycat.config.loader.zkprocess.parse.ParseJsonServiceInf;
import io.mycat.config.loader.zkprocess.parse.ParseXmlServiceInf;
import io.mycat.config.loader.zkprocess.parse.XmlProcessBase;
import io.mycat.config.loader.zkprocess.parse.entryparse.rule.json.FunctionJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.rule.json.TableRuleJsonParse;
import io.mycat.config.loader.zkprocess.parse.entryparse.rule.xml.RuleParseXmlImpl;
import io.mycat.config.loader.zkprocess.zookeeper.DataInf;
import io.mycat.config.loader.zkprocess.zookeeper.DiretoryInf;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkDirectoryImpl;
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import io.mycat.util.KVPathUtil;
import io.mycat.util.ResourceUtil;

/**
 * RuleszkToxmlLoader
 *
 *
 * author:liujun
 * Created:2016/9/15
 *
 *
 *
 *
 */
public class RuleszkToxmlLoader extends ZkMultLoader implements NotifyService {


    private static final Logger LOGGER = LoggerFactory.getLogger(RuleszkToxmlLoader.class);

    private final String currZkPath;

    private static final String WRITEPATH = "rule.xml";

    private ParseXmlServiceInf<Rules> parseRulesXMl;

    private ParseJsonServiceInf<List<TableRule>> parseJsonTableRuleService = new TableRuleJsonParse();

    private ParseJsonServiceInf<List<Function>> parseJsonFunctionService = new FunctionJsonParse();

    public RuleszkToxmlLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
                              XmlProcessBase xmlParseBase, ConfigStatusListener confListener) {
        this.setCurator(curator);
        currZkPath = KVPathUtil.getConfRulePath();
        zookeeperListen.addToInit(this);
        parseRulesXMl = new RuleParseXmlImpl(xmlParseBase);
        confListener.addChild(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        DiretoryInf rulesDirectory = new ZkDirectoryImpl(currZkPath, null);
        this.getTreeDirectory(currZkPath, KVPathUtil.RULES, rulesDirectory);

        ZkDirectoryImpl zkDirectory = (ZkDirectoryImpl) rulesDirectory.getSubordinateInfo().get(0);
        Rules rules = this.zktoRulesBean(zkDirectory);

        LOGGER.info("RuleszkToxmlLoader notifyProcess zk to object  zk Rules Object  :" + rules);

        writeMapFileAddFunction(rules.getFunction());

        LOGGER.info("RuleszkToxmlLoader notifyProcess write mapFile is success ");

        String path = ResourceUtil.getResourcePathFromRoot(ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey());
        path = new File(path).getPath() + File.separator;
        path = path + WRITEPATH;

        LOGGER.info("RuleszkToxmlLoader notifyProcess zk to object writePath :" + path);

        this.parseRulesXMl.parseToXmlWrite(rules, path, "rule");

        LOGGER.info("RuleszkToxmlLoader notifyProcess zk to object zk Rules      write :" + path + " is success");

        return true;
    }

    /**
     * zktoRulesBean
     *
     * @param zkDirectory
     * @return
     * @Created 2016/9/17
     */
    private Rules zktoRulesBean(DiretoryInf zkDirectory) {
        Rules rules = new Rules();

        // tablerule
        DataInf rulesZkData = this.getZkData(zkDirectory, KVPathUtil.TABLE_RULE);
        List<TableRule> tableRuleData = parseJsonTableRuleService.parseJsonToBean(rulesZkData.getDataValue());
        rules.setTableRule(tableRuleData);

        // function
        DataInf functionZkData = this.getZkData(zkDirectory, KVPathUtil.FUNCTION);
        List<Function> functionList = parseJsonFunctionService.parseJsonToBean(functionZkData.getDataValue());
        rules.setFunction(functionList);

        return rules;
    }

    /**
     * writeMapFileAddFunction
     *
     * @param functionList
     * @Created 2016/9/18
     */
    private void writeMapFileAddFunction(List<Function> functionList) {

        List<Property> tempData = new ArrayList<>();

        List<Property> writeData = new ArrayList<>();

        for (Function function : functionList) {
            List<Property> proList = function.getProperty();
            if (null != proList && !proList.isEmpty()) {
                for (Property property : proList) {
                    if (ParseParamEnum.ZK_PATH_RULE_MAPFILE_NAME.getKey().equals(property.getName())) {
                        tempData.add(property);
                    }
                }

                if (!tempData.isEmpty()) {
                    for (Property property : tempData) {
                        for (Property prozkdownload : proList) {
                            if (property.getValue().equals(prozkdownload.getName())) {
                                writeData.add(prozkdownload);
                            }
                        }
                    }
                }

                if (!writeData.isEmpty()) {
                    for (Property writeMsg : writeData) {
                        try {
                            ConfFileRWUtils.writeFile(writeMsg.getName(), writeMsg.getValue());
                        } catch (IOException e) {
                            LOGGER.error("RuleszkToxmlLoader write File IOException", e);
                        }
                    }
                }

                proList.removeAll(writeData);

                tempData.clear();
                writeData.clear();
            }
        }

    }
}
