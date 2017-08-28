package io.mycat.config.loader.zkprocess.xmltozk.listen;


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
import io.mycat.config.loader.zkprocess.zookeeper.process.ZkMultLoader;
import io.mycat.util.KVPathUtil;

/**
 * RulesxmlTozkLoader
 *
 *
 * author:liujun
 * Created:2016/9/15
 *
 *
 *
 *
 */
public class RulesxmlTozkLoader extends ZkMultLoader implements NotifyService {


    private static final Logger LOGGER = LoggerFactory.getLogger(RulesxmlTozkLoader.class);

    private final String currZkPath;

    private static final String RULE_PATH = ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey() + "rule.xml";

    private ParseXmlServiceInf<Rules> parseRulesXMl;

    private ParseJsonServiceInf<List<TableRule>> parseJsonTableRuleService = new TableRuleJsonParse();

    private ParseJsonServiceInf<List<Function>> parseJsonFunctionService = new FunctionJsonParse();

    public RulesxmlTozkLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
                              XmlProcessBase xmlParseBase) {
        this.setCurator(curator);
        currZkPath = KVPathUtil.getConfRulePath();
        zookeeperListen.addToInit(this);
        parseRulesXMl = new RuleParseXmlImpl(xmlParseBase);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        Rules rules = this.parseRulesXMl.parseXmlToBean(RULE_PATH);
        LOGGER.info("RulesxmlTozkLoader notifyProcess xml to zk Rules Object  :" + rules);
        this.xmlTozkRulesJson(currZkPath, rules);

        LOGGER.info("RulesxmlTozkLoader notifyProcess xml to zk is success");

        return true;
    }

    /**
     * xmlTozkRulesJson
     *
     * @param basePath
     * @param rules
     * @throws Exception
     * @Created 2016/9/17
     */
    private void xmlTozkRulesJson(String basePath, Rules rules) throws Exception {
        String tableRuleJson = this.parseJsonTableRuleService.parseBeanToJson(rules.getTableRule());
        this.checkAndwriteString(basePath, KVPathUtil.TABLE_RULE, tableRuleJson);

        this.readMapFileAddFunction(rules.getFunction());

        String functionJson = this.parseJsonFunctionService.parseBeanToJson(rules.getFunction());
        this.checkAndwriteString(basePath, KVPathUtil.FUNCTION, functionJson);
    }

    /**
     * readMapFileAddFunction
     *
     * @param functionList
     * @Created 2016/9/18
     */
    private void readMapFileAddFunction(List<Function> functionList) {

        List<Property> tempData = new ArrayList<>();

        for (Function function : functionList) {
            List<Property> proList = function.getProperty();
            if (null != proList && !proList.isEmpty()) {
                for (Property property : proList) {
                    // if mapfile,read and save to json
                    if (ParseParamEnum.ZK_PATH_RULE_MAPFILE_NAME.getKey().equals(property.getName())) {
                        Property mapFilePro = new Property();
                        mapFilePro.setName(property.getValue());
                        try {
                            mapFilePro.setValue(ConfFileRWUtils.readFile(property.getValue()));
                            tempData.add(mapFilePro);
                        } catch (IOException e) {
                            LOGGER.error("RulesxmlTozkLoader readMapFile IOException", e);
                        }
                    }
                }
                proList.addAll(tempData);
                tempData.clear();
            }
        }
    }
}
