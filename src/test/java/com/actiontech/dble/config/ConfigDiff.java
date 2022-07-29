package com.actiontech.dble.config;

import com.actiontech.dble.backend.datasource.PhysicalDbGroup;
import com.actiontech.dble.backend.datasource.PhysicalDbInstance;
import com.actiontech.dble.config.model.db.PoolConfig;
import com.actiontech.dble.config.model.db.DbGroupConfig;
import com.actiontech.dble.config.model.db.DbInstanceConfig;
import com.actiontech.dble.config.model.user.AnalysisUserConfig;
import com.actiontech.dble.config.model.user.UserName;
import com.actiontech.dble.config.model.user.ServerUserConfig;
import com.actiontech.dble.config.model.user.SingleDbGroupUserConfig;
import com.actiontech.dble.config.model.user.ShardingUserConfig;
import com.actiontech.dble.config.model.user.ManagerUserConfig;
import com.actiontech.dble.config.model.user.RwSplitUserConfig;
import com.actiontech.dble.backend.datasource.ShardingNode;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class ConfigDiff {

    /**
     * requires attention:
     * New fields need to consider equals and copyBaseInfo methods
     * http://10.186.18.11/jira/browse/DBLE0REQ-1793?focusedCommentId=99601&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-99601
     */

    @Test
    public void testDataBaseType() {
        List<Field> newFieldList = getAllFields(DataBaseType.class);
        List<String> newFieldNameList = newFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        List<Field> oldFieldList = getAllFields(com.actiontech.dble.config.DataBaseType.class);
        List<String> oldFieldNameList = oldFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        Assert.assertEquals(newFieldNameList, oldFieldNameList);
    }

    @Test
    public void testPoolConfig() {
        List<Field> newFieldList = getAllFields(PoolConfig.class);
        List<String> newFieldNameList = newFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        List<Field> oldFieldList = getAllFields(com.actiontech.dble.config.PoolConfig.class);
        List<String> oldFieldNameList = oldFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        Assert.assertEquals(newFieldNameList, oldFieldNameList);
    }

    @Test
    public void testDbGroupConfig() {
        List<Field> newFieldList = getAllFields(DbGroupConfig.class);
        List<String> newFieldNameList = newFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        List<Field> oldFieldList = getAllFields(com.actiontech.dble.config.DbGroupConfig.class);
        List<String> oldFieldNameList = oldFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        Assert.assertEquals(newFieldNameList, oldFieldNameList);
    }

    @Test
    public void testDbInstanceConfig() {
        List<Field> newFieldList = getAllFields(DbInstanceConfig.class);
        List<String> newFieldNameList = newFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        List<Field> oldFieldList = getAllFields(com.actiontech.dble.config.DbInstanceConfig.class);
        List<String> oldFieldNameList = oldFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        Assert.assertEquals(newFieldNameList, oldFieldNameList);
    }

    @Test
    public void testDbGroup() {
        List<Field> newFieldList = getAllFields(PhysicalDbGroup.class);
        List<String> newFieldNameList = newFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        List<Field> oldFieldList = getAllFields(com.actiontech.dble.config.PhysicalDbGroup.class);
        List<String> oldFieldNameList = oldFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        Assert.assertEquals(newFieldNameList, oldFieldNameList);
    }

    @Test
    public void testDbInstance() {
        List<Field> newFieldList = getAllFields(PhysicalDbInstance.class);
        List<String> newFieldNameList = newFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        List<Field> oldFieldList = getAllFields(com.actiontech.dble.config.PhysicalDbInstance.class);
        List<String> oldFieldNameList = oldFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        Assert.assertEquals(newFieldNameList, oldFieldNameList);
    }


    @Test
    public void testUserName() {
        List<Field> newFieldList = getAllFields(UserName.class);
        List<String> newFieldNameList = newFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        List<Field> oldFieldList = getAllFields(com.actiontech.dble.config.UserName.class);
        List<String> oldFieldNameList = oldFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        Assert.assertEquals(newFieldNameList, oldFieldNameList);
    }

    @Test
    public void testServerUserConfig() {
        List<Field> newFieldList = getAllFields(ServerUserConfig.class);
        List<String> newFieldNameList = newFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        List<Field> oldFieldList = getAllFields(com.actiontech.dble.config.ServerUserConfig.class);
        List<String> oldFieldNameList = oldFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        Assert.assertEquals(newFieldNameList, oldFieldNameList);
    }

    @Test
    public void testSingleDbGroupUserConfig() {
        List<Field> newFieldList = getAllFields(SingleDbGroupUserConfig.class);
        List<String> newFieldNameList = newFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        List<Field> oldFieldList = getAllFields(com.actiontech.dble.config.SingleDbGroupUserConfig.class);
        List<String> oldFieldNameList = oldFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        Assert.assertEquals(newFieldNameList, oldFieldNameList);
    }

    @Test
    public void testShardingUserConfig() {
        List<Field> newFieldList = getAllFields(ShardingUserConfig.class);
        List<String> newFieldNameList = newFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        List<Field> oldFieldList = getAllFields(com.actiontech.dble.config.ShardingUserConfig.class);
        List<String> oldFieldNameList = oldFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        Assert.assertEquals(newFieldNameList, oldFieldNameList);
    }

    @Test
    public void testManagerUserConfig() {
        List<Field> newFieldList = getAllFields(ManagerUserConfig.class);
        List<String> newFieldNameList = newFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        List<Field> oldFieldList = getAllFields(com.actiontech.dble.config.ManagerUserConfig.class);
        List<String> oldFieldNameList = oldFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        Assert.assertEquals(newFieldNameList, oldFieldNameList);
    }

    @Test
    public void testAnalysisUserConfig() {
        List<Field> newFieldList = getAllFields(AnalysisUserConfig.class);
        List<String> newFieldNameList = newFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        List<Field> oldFieldList = getAllFields(com.actiontech.dble.config.AnalysisUserConfig.class);
        List<String> oldFieldNameList = oldFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        Assert.assertEquals(newFieldNameList, oldFieldNameList);
    }

    @Test
    public void testRwSplitUserConfig() {
        List<Field> newFieldList = getAllFields(RwSplitUserConfig.class);
        List<String> newFieldNameList = newFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        List<Field> oldFieldList = getAllFields(com.actiontech.dble.config.RwSplitUserConfig.class);
        List<String> oldFieldNameList = oldFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        Assert.assertEquals(newFieldNameList, oldFieldNameList);
    }

    @Test
    public void testShardingNode() {
        List<Field> newFieldList = getAllFields(ShardingNode.class);
        List<String> newFieldNameList = newFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        List<Field> oldFieldList = getAllFields(com.actiontech.dble.config.ShardingNode.class);
        List<String> oldFieldNameList = oldFieldList.stream()
                .map(field -> field.getName())
                .collect(Collectors.toList());
        Assert.assertEquals(newFieldNameList, oldFieldNameList);
    }

    public List<Field> getAllFields(Class clazz) {
        List<Field> allFields = new ArrayList<>();
        allFields.addAll(Arrays.asList(clazz.getDeclaredFields()));

        Class clazzSuper = clazz.getSuperclass();
        while (clazzSuper != Object.class) {
            allFields.addAll(Arrays.asList(clazzSuper.getDeclaredFields()));
            clazzSuper = clazzSuper.getSuperclass();
        }
        return allFields;
    }
}
