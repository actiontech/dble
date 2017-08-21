package io.mycat.config.loader.zkprocess.zktoxml.listen;

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
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 进行rule的文件从zk中加载
 * 源文件名：RuleszkToxmlLoader.java
 * 文件版本：1.0.0
 * 创建作者：liujun
 * 创建日期：2016年9月15日
 * 修改作者：liujun
 * 修改日期：2016年9月15日
 * 文件描述：TODO
 * 版权所有：Copyright 2016 zjhz, Inc. All Rights Reserved.
 */
public class RuleszkToxmlLoader extends ZkMultLoader implements NotifyService {

    /**
     * 日志
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(RuleszkToxmlLoader.class);

    /**
     * 当前文件中的zkpath信息
     */
    private final String currZkPath;

    /**
     * 写入本地的文件路径
     */
    private static final String WRITEPATH = "rule.xml";

    /**
     * Rules的xml的转换信息
     */
    private ParseXmlServiceInf<Rules> parseRulesXMl;

    /**
     * 表的路由信息
     */
    private ParseJsonServiceInf<List<TableRule>> parseJsonTableRuleService = new TableRuleJsonParse();

    /**
     * 表对应的字段信息
     */
    private ParseJsonServiceInf<List<Function>> parseJsonFunctionService = new FunctionJsonParse();

    public RuleszkToxmlLoader(ZookeeperProcessListen zookeeperListen, CuratorFramework curator,
                              XmlProcessBase xmlParseBase, ConfigStatusListener confListener) {
        this.setCurator(curator);
        currZkPath = KVPathUtil.getConfRulePath();
        // 将当前自己注册为事件接收对象
        zookeeperListen.addToInit(this);
        // 生成xml与类的转换信息
        parseRulesXMl = new RuleParseXmlImpl(xmlParseBase);
        confListener.addChild(this);
    }

    @Override
    public boolean notifyProcess() throws Exception {
        // 1,将集群Rules目录下的所有集群按层次结构加载出来
        // 通过组合模式进行zk目录树的加载
        DiretoryInf RulesDirectory = new ZkDirectoryImpl(currZkPath, null);
        // 进行递归的数据获取
        this.getTreeDirectory(currZkPath, KVPathUtil.RULES, RulesDirectory);

        // 从当前的下一级开始进行遍历,获得到
        ZkDirectoryImpl zkDirectory = (ZkDirectoryImpl) RulesDirectory.getSubordinateInfo().get(0);
        Rules Rules = this.zktoRulesBean(zkDirectory);

        LOGGER.info("RuleszkToxmlLoader notifyProcess zk to object  zk Rules Object  :" + Rules);

        // 将mapfile信息写入到文件 中
        writeMapFileAddFunction(Rules.getFunction());

        LOGGER.info("RuleszkToxmlLoader notifyProcess write mapFile is success ");

        // 数配制信息写入文件
        String path = ResourceUtil.getResourcePathFromRoot(ZookeeperPath.ZK_LOCAL_WRITE_PATH.getKey());
        path = new File(path).getPath() + File.separator;
        path = path + WRITEPATH;

        LOGGER.info("RuleszkToxmlLoader notifyProcess zk to object writePath :" + path);

        this.parseRulesXMl.parseToXmlWrite(Rules, path, "rule");

        LOGGER.info("RuleszkToxmlLoader notifyProcess zk to object zk Rules      write :" + path + " is success");

        return true;
    }

    /**
     * 将zk上面的信息转换为javabean对象
     * 方法描述
     *
     * @param zkDirectory
     * @return
     * @创建日期 2016年9月17日
     */
    private Rules zktoRulesBean(DiretoryInf zkDirectory) {
        Rules Rules = new Rules();

        // tablerule信息
        DataInf RulesZkData = this.getZkData(zkDirectory, KVPathUtil.TABLE_RULE);
        List<TableRule> tableRuleData = parseJsonTableRuleService.parseJsonToBean(RulesZkData.getDataValue());
        Rules.setTableRule(tableRuleData);

        // 得到function信息
        DataInf functionZkData = this.getZkData(zkDirectory, KVPathUtil.FUNCTION);
        List<Function> functionList = parseJsonFunctionService.parseJsonToBean(functionZkData.getDataValue());
        Rules.setFunction(functionList);

        return Rules;
    }

    /**
     * 读取序列配制文件便利店
     * 方法描述
     *
     * @param functionList
     * @创建日期 2016年9月18日
     */
    private void writeMapFileAddFunction(List<Function> functionList) {

        List<Property> tempData = new ArrayList<>();

        List<Property> writeData = new ArrayList<>();

        for (Function function : functionList) {
            List<Property> proList = function.getProperty();
            if (null != proList && !proList.isEmpty()) {
                // 进行数据遍历
                for (Property property : proList) {
                    // 如果为mapfile，则需要去读取数据信息，并存到json中
                    if (ParseParamEnum.ZK_PATH_RULE_MAPFILE_NAME.getKey().equals(property.getName())) {
                        tempData.add(property);
                    }
                }

                // 通过mapfile的名称，找到对应的数据信息
                if (!tempData.isEmpty()) {
                    for (Property property : tempData) {
                        for (Property prozkdownload : proList) {
                            // 根据mapfile的文件名去提取数据
                            if (property.getValue().equals(prozkdownload.getName())) {
                                writeData.add(prozkdownload);
                            }
                        }
                    }
                }

                // 将对应的数据信息写入到磁盘中
                if (!writeData.isEmpty()) {
                    for (Property writeMsg : writeData) {
                        try {
                            ConfFileRWUtils.writeFile(writeMsg.getName(), writeMsg.getValue());
                        } catch (IOException e) {
                            LOGGER.error("RuleszkToxmlLoader write File IOException", e);
                        }
                    }
                }

                // 将数据添加的集合中
                proList.removeAll(writeData);

                // 清空，以进行下一次的添加
                tempData.clear();
                writeData.clear();
            }
        }

    }
}
