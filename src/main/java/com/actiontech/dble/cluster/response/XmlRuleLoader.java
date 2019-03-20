/*
 * Copyright (C) 2016-2019 ActionTech.
 * License: http://www.gnu.org/licenses/gpl.html GPL version 2 or higher.
 */

package com.actiontech.dble.cluster.response;

import com.actiontech.dble.cluster.ClusterGeneralConfig;
import com.actiontech.dble.cluster.ClusterHelper;
import com.actiontech.dble.cluster.ClusterParamCfg;
import com.actiontech.dble.cluster.ClusterPathUtil;
import com.actiontech.dble.cluster.bean.KvBean;
import com.actiontech.dble.cluster.listener.ClusterClearKeyListener;
import com.actiontech.dble.config.loader.zkprocess.comm.ConfFileRWUtils;
import com.actiontech.dble.config.loader.zkprocess.console.ParseParamEnum;
import com.actiontech.dble.config.loader.zkprocess.entity.Property;
import com.actiontech.dble.config.loader.zkprocess.entity.Rules;
import com.actiontech.dble.config.loader.zkprocess.entity.rule.function.Function;
import com.actiontech.dble.config.loader.zkprocess.entity.rule.tablerule.TableRule;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseJsonServiceInf;
import com.actiontech.dble.config.loader.zkprocess.parse.ParseXmlServiceInf;
import com.actiontech.dble.config.loader.zkprocess.parse.XmlProcessBase;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.rule.json.FunctionJsonParse;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.rule.json.TableRuleJsonParse;
import com.actiontech.dble.config.loader.zkprocess.parse.entryparse.rule.xml.RuleParseXmlImpl;
import com.actiontech.dble.util.ResourceUtil;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by szf on 2018/1/26.
 */
public class XmlRuleLoader implements ClusterXmlLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterXmlLoader.class);

    private ParseJsonServiceInf<List<TableRule>> parseJsonTableRuleService = new TableRuleJsonParse();

    private ParseJsonServiceInf<List<Function>> parseJsonFunctionService = new FunctionJsonParse();

    private static final String WRITEPATH = "rule.xml";

    private ParseXmlServiceInf<Rules> parseRulesXMl;

    private static final String CONFIG_PATH = ClusterPathUtil.getConfRulePath();


    public XmlRuleLoader(XmlProcessBase xmlParseBase, ClusterClearKeyListener confListener) {
        this.parseRulesXMl = new RuleParseXmlImpl(xmlParseBase);
        confListener.addChild(this, CONFIG_PATH);
    }

    @Override
    public void notifyProcess(KvBean configValue) throws Exception {
        LOGGER.info("notify " + configValue.getKey() + " " + configValue.getValue() + " " + configValue.getChangeType());
        KvBean lock = ClusterHelper.getKV(ClusterPathUtil.getConfChangeLockPath());
        if (ClusterGeneralConfig.getInstance().getValue(ClusterParamCfg.CLUSTER_CFG_MYID).equals(lock.getValue())) {
            return;
        }
        Rules rule = new Rules();
        //the config Value in ucore is an all in one json config of the schema.xml
        JSONObject jsonObj = JSONObject.parseObject(configValue.getValue());

        List<Function> functions = parseJsonFunctionService.parseJsonToBean(jsonObj.getJSONArray(ClusterPathUtil.FUNCTION).toJSONString());
        rule.setFunction(functions);

        List<TableRule> tableRules = parseJsonTableRuleService.parseJsonToBean(jsonObj.getJSONArray(ClusterPathUtil.TABLE_RULE).toJSONString());
        rule.setTableRule(tableRules);

        rule.setVersion(jsonObj.getString(ClusterPathUtil.VERSION));

        String path = ResourceUtil.getResourcePathFromRoot(ClusterPathUtil.UCORE_LOCAL_WRITE_PATH);
        path = new File(path).getPath() + File.separator;
        path += WRITEPATH;

        LOGGER.info("SchemasLoader notifyProcess ucore to object writePath :" + path);

        writeMapFileAddFunction(functions);

        this.parseRulesXMl.parseToXmlWrite(rule, path, "rule");

        LOGGER.info("SchemasLoader notifyProcess ucore to object zk schema      write :" + path + " is success");
    }

    @Override
    public void notifyCluster() throws Exception {
        Rules rules = this.parseRulesXMl.parseXmlToBean(ClusterPathUtil.UCORE_LOCAL_WRITE_PATH + WRITEPATH);
        JSONObject rule = new JSONObject();

        readMapFileAddFunction(rules.getFunction());
        rule.put(ClusterPathUtil.VERSION, rules.getVersion());
        rule.put(ClusterPathUtil.TABLE_RULE, rules.getTableRule());
        rule.put(ClusterPathUtil.FUNCTION, rules.getFunction());
        ClusterHelper.setKV(CONFIG_PATH, rule.toJSONString());
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
                            LOGGER.warn("RulesxmlTozkLoader readMapFile IOException", e);
                        }
                    }
                }
                proList.addAll(tempData);
                tempData.clear();
            }
        }
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
                            LOGGER.warn("RuleszkToxmlLoader write File IOException", e);
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
