
date:2016/12/14 
VERSION 0.2.1  
CONTENT:  
### 1.数据库写入
#### 1.1 普通事务实现分布式事务 及日志
#### 1.2 XA实现分布式(事务MySQL5.7以上) 及recover
#### 1.3 隐式分布式事务 及日志

分布式事务使用两阶段提交的方式,
具体设计 参见 http://10.186.18.11/confluence/pages/viewpage.action?pageId=3670789

###  2 meta 数据定时巡检
参见 http://10.186.18.11/confluence/pages/viewpage.action?pageId=3670785

###  3增加可选配置项 
server.xml 文件中  system标签 下
 
```
<property name="xaSessionCheckPeriod">1000</property><!-- 使用XA时，如果发生server导致的数据不一致，定期进行重试commit或者rollback，默认1000毫秒-->
<property name="xaLogCleanPeriod">1000</property <!-- 使用XA时，已经完结的XA LOG 会定时移除，默认1000毫秒 -->
<property name="checkTableConsistency">0</property><!-- 是否进行meta数据定时巡检，默认不启用 -->
<property name="checkTableConsistencyPeriod"> 60000</property><!-- meta数据定时巡检周期，默认60000毫秒 -->
```

也可见server_template.xml 

 
------
------

date:2016/11/28  
VERSION 0.1.1  
CONTENT:  
### 1.拆分算法

保留     
- 枚举方式分区  
- 数字范围方式分区  
- 固定Hash 分区  
- 固定Hash 分区（string 类型）
- 按日期分区

以上修改部分  
固定Hash 分区:
sum((count[i]*length[i])) 不受乘积为1024的限制，改为不大于1024的都可以支持  
按日期分区:  
可以设置default node  

其余配置见rule_template.xml  

### 2.支持insert 不带columns
