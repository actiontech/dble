package com.actiontech.dble.config.loader.ucoreprocess.loader;

import com.actiontech.dble.config.loader.ucoreprocess.*;
import com.actiontech.dble.config.loader.ucoreprocess.bean.UKvBean;
import com.actiontech.dble.config.loader.ucoreprocess.listen.UcoreClearKeyListener;
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
import java.util.List;

/**
 * Created by szf on 2018/1/26.
 */
public class UXmlRuleLoader implements UcoreXmlLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(UcoreXmlLoader.class);

    private ParseJsonServiceInf<List<TableRule>> parseJsonTableRuleService = new TableRuleJsonParse();

    private ParseJsonServiceInf<List<Function>> parseJsonFunctionService = new FunctionJsonParse();

    private static final String WRITEPATH = "rule.xml";

    private ParseXmlServiceInf<Rules> parseRulesXMl;

    private static final String CONFIG_PATH = UcorePathUtil.getConfRulePath();



    public UXmlRuleLoader(XmlProcessBase xmlParseBase, UcoreClearKeyListener confListener) {
        this.parseRulesXMl = new RuleParseXmlImpl(xmlParseBase);
        confListener.addChild(this, CONFIG_PATH);
    }

    @Override
    public void notifyProcess(UKvBean configValue) throws Exception {

        UKvBean lock = ClusterUcoreSender.getKey(UcorePathUtil.getConfChangeLockPath());
        if (UcoreConfig.getInstance().getValue(UcoreParamCfg.UCORE_CFG_MYID).equals(lock.getValue())) {
            return;
        }
        Rules rule = new Rules();
        //the config Value in ucore is an all in one json config of the schema.xml
        JSONObject jsonObj = JSONObject.parseObject(configValue.getValue());

        List<Function> functions = parseJsonFunctionService.parseJsonToBean(jsonObj.getJSONArray(UcorePathUtil.FUNCTION).toJSONString());
        rule.setFunction(functions);

        List<TableRule> tableRules = parseJsonTableRuleService.parseJsonToBean(jsonObj.getJSONArray(UcorePathUtil.TABLE_RULE).toJSONString());
        rule.setTableRule(tableRules);

        String path = ResourceUtil.getResourcePathFromRoot(UcorePathUtil.UCORE_LOCAL_WRITE_PATH);
        path = new File(path).getPath() + File.separator;
        path += WRITEPATH;

        LOGGER.info("SchemasLoader notifyProcess ucore to object writePath :" + path);

        this.parseRulesXMl.parseToXmlWrite(rule, path, "rule");

        LOGGER.info("SchemasLoader notifyProcess ucore to object zk schema      write :" + path + " is success");
    }

    @Override
    public void notifyCluster() throws Exception {
        Rules rules = this.parseRulesXMl.parseXmlToBean(UcorePathUtil.UCORE_LOCAL_WRITE_PATH + WRITEPATH);
        JSONObject rule = new JSONObject();
        rule.put(UcorePathUtil.TABLE_RULE, rules.getTableRule());
        rule.put(UcorePathUtil.FUNCTION, rules.getFunction());
        ClusterUcoreSender.sendDataToUcore(CONFIG_PATH, rule.toJSONString());
    }

}
