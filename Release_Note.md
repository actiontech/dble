DATE:2017/03/21  
VERSION 0.3.1  
CONTENT:  
### 1.fix #32, #36, #37, #39 
### 2.new issue 33
### 3 fix 46 
将连接登陆成功和关闭的日志改为debug级别，另外更新以下jar包，防止写日志时生产者过多导致disruptor内部环形队列越界死锁。   
disruptor-3.3.4.jar->disruptor-3.3.6.jar  
log4j-1.2-api-2.5.jar->log4j-1.2-api-2.7.jar  
log4j-api-2.5.jar->log4j-api-2.7.jar  
log4j-core-2.5.jar->log4j-core-2.7.jar  
log4j-slf4j-impl-2.5.jar->log4j-slf4j-impl-2.7.jar  

### 4.聚合函数相关bug： #50, #45, #44, #41, #31
测试select 中包含聚合函数时请将```<property name="useExtensions">true</property>``` 打开看是否解决，并回归其他聚合函数的case。 
 

### 5 join、union、子查询语句  #47 
测试时请将
```
<property name="useExtensions">true</property>
```
 打开看是否解决。

------

date:2017/03/11 
VERSION 0.2.6  
CONTENT:  
### 1.fix 29  
select @@session.tx_isolation; 写死的问题  
### 2. set 
无论是否支持，都返回OK--->不支持的返回错误提示  
### 3.fix 31
 聚合函数min不正确  
### 4. 连接处理核心 
改成有界队列（防止大量短连接并发导致 开太多线程）
### 5.查询分析树 相关 (暂时无需测试，未暴露给用户)
	5.1 GLOBAL TABLE 
	5.2 druid 更新
	5.3 查询分析树一些bug
	
### 6.将join时候mycat原有逻辑暴露出来。
修正当sql带schema时候可能会找不到路由的bug 

### 7.druid-1.0.26.jar 升级到 druid-1.0.28.jar

------


date:2017/03/11 
VERSION 0.2.5  
CONTENT:  
### 1.移除schema 的以下配置：checkSQLschema   
即支持sql 在未使用use schema时，可以用schema.table 来作查询，增删改。  

例如 ：原来   
use db1；  
insert into table1 values 。。。。   
现在：
insert into db1.table1 values 。。。。  

### 2.将schema 下lowercase配置 移到全局。  

方法： 在server.xml 的  
<system>下配置  
```
	<property name="lowerCaseTableNames"> true</property>  <!-- true 和不配为大小写不敏感,  所有配了非true的值表示大小写敏感-->  
```
和mysql内部处理方式相同，当大小写不敏感时，内存中全部用小写来做比较。  
 
### 3. 对解析性能优化，对SQL的多次解析尽量 合在一起   
### 4. 将所有未经测试的改进/新功能都隐藏起来，如需测试可配置一个内部参数尝试新功能，等稳定后再放出。  
（本次主要是复杂SQL的查询，还未完全写完，本次测试暂不涉及）。   
启用方法，配：<property name="useExtensions">true</property>，限于内部使用。  


升级: 
从 0.2.4升级上来 需替换jar包,schema.dtd , 以及去掉schema.xml 配置中的checkSQLschema   

------

date:2017/02/08 
VERSION 0.2.4  
CONTENT:  
### 1.fix bugs
issue 24,25,26,13 

@maguoji:
when multi writing masters and a writing master that is not the last don't have a slave, master don't correspond to its slaves

### 2.合并官方bug 
 1126, 1125，1294,(1248,1251 and new $ bug//官方修复不完全正确 )
参见 issue 27

### 3.不支持  MultiQueries，会提示警告。  
### 4.性能方面：
Reactor 改为非阻塞  
线程池增加cached  
xa recover log 写入性能增强  


date:2017/01/16 
VERSION 0.2.3  
CONTENT:  
### 1.fix bugs
issue 5.  
isseu 6:  支持大小写敏感，粒度：schema。  
方法:schema.xml中 标签schema下 添加lowerCase 属性 0 或者 1 ,默认不添加为为1大小写不敏感。  
issue 7.  
issue 8.  
issue 10.  
ISSUE 16.  

### 2.移除对其他后端异构数据库的支持,以及对应的配置中的无用项。  
### 3.不支持 between and 的枚举和 固定Hash 分区（string 类型）算法会提示警告。  
### 4.DML(I/U/D)以及DDL的描述和限制
参见http://10.186.18.11/confluence/pages/viewpage.action?pageId=3671908 


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
<property name="recordTxn">0</property>  <!-- 1为开启事务日志记录、0为关闭-->
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
