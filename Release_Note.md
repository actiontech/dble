DATE:2017/06/29  
VERSION 2.17.06.0  
CONTENT:  
## 1.feature
1.移除writeType参数，等效于原来writeType =0  
2.conf/index_to_charset.properties的内容固化到代码 #77     
3.show @@binlog.status  ，显示节点间事务一致的binlog线 #118   
4.增强explain执行计划#143   
5.show tables时,不显示未创建的表，show full tables 拆分表的type会显示为SHARDING TABLE，global表会显示为GLOBAL TABLE  
6.ddl 执行前做一次心跳检查  
7.多表查询中using()结果未合并重复列 #103  
8.natural join #97  
## 2.fix bugs  
1.配置为自循环不会去拉取meta信息 #146  
2.自增序列部分算法缺陷及改进  
3.单节点查询不加入主键缓存bug #160   
4.租户权限不不隔离的问题 #164  
5.show full tables from db，当db为不存在的db时，多发包导致乱序  #159   
6.数字范围算法(AutoPartitionByLong),有默认节点时，between...and...可能路由错误 #145
## 3.不兼容项
#### 3.1 全局序列配置 ：
##### 3.1.1 sequence_conf.properties 
库表名要用"\`"包起来,用"."连接  
##### 3.1.2 sequence_db_conf.properties  
库表名要用"\`"包起来,用"."连接  
#### 3.2 缓存配置
3.2.1 cacheservice.properties 库表名要用"\`"包起来，用"_"连接。  
#### 3.3 自增序列部分算法  
##### 3.3.1.本地时间戳方式  
ID= (30(毫秒时间戳前30位)+5(机器ID)+5(业务编码)+12(重复累加)+12(毫秒时间戳后12位)  
##### 3.3.2.分布式ZK ID 生成器     
ID= 63位二进制 (9(线程ID) +5(实例ID)+4(机房ID)+6(重复累加) +39(毫秒求模,可用17年))  
#### 3.4 server.xml  
3.4.1 mycatNodeId 改为serverNodeId  

## 4.ushard分支 #117 
#### 4.1 客户端登录信息 
显示ushard  
#### 4.2 注解
原本用mycat的请用ushard  
#### 4.3 自增函数 
dbseq.sql 已经重新生成，mycat已经用ushard替换  
#### 4.4 配置
##### 4.4.1 xml的使用规约  
例如：server的根节点写法是：  

```
<!DOCTYPE ushard:server SYSTEM "server.dtd">
```  

```
<ushard:server xmlns:ushard="http://io.ushard/">
```  

##### 4.4.2 cacheservice.properties 
此配置中需要指定类名用于反射加载缓存池类型，可使用简称   
ehcache  
leveldb  
mapdb  
来指代原有类名(不区分大小写)，mycat分支下兼容原有方式和简称方式。  
原有方式如下，按顺序与简称方式一一对应。  
io.mycat.cache.impl.EnchachePooFactory  
io.mycat.cache.impl.LevelDBCachePooFactory  
io.mycat.cache.impl.MapDBCachePooFactory   
和简称方式。  
##### 4.4.3 rule.xml  
rule.xml配置当中需要指定类名用于反射加载拆分算法，可使用简称    
Hash  
StringHash  
Enum  
NumberRange  
Date  
PatternRange  
来指代原有类名(不区分大小写)，mycat分支下兼容原有方式和简称方式。  
原有方式如下，按顺序与简称方式一一对应。  
io.mycat.route.function.PartitionByLong(固定Hash 分区)  
io.mycat.route.function.PartitionByString(String固定Hash 分区)   
io.mycat.route.function.PartitionByFileMap(枚举方式)  
io.mycat.route.function.AutoPartitionByLong(数字范围)  
io.mycat.route.function.PartitionByDate(日期分区)  
io.mycat.route.function.PartitionByPattern(取模范围约束)  

#### 4.5 日志
日志路径logs/ushard.log 里面涉及到的包名从io.mycat 改为com.actionsky.com
   
------   
  
 

DATE:2017/04/20  
VERSION 2.17.04.0  
CONTENT:  
同开源1.6.1版本相比，做了如下更新  
## 1.fix bug（只列了重要的）  
#### 1.1 拆分算法  
hash拆分算法中,如果使用between跨越了所有分区，计算时分区会不正确  
范围拆分算法中，没有超越范围的默认结点。  
#### 1.2 XA实现分布式事务不可用，客户端报错退出  
#### 1.3 不当的线程并发导致double free,服务崩溃，被守护进程重启   
#### 1.4 防火墙，相同的sql因为缓存，防火墙会失效  
#### 1.5 聚合函数，group by，order by ，复杂查询等多处bug
这部分移植了ares的代码，文档待补充     
#### 1.6 普通用户都具有管理权限 

## 2.feature  
#### 2.1 拆分算法

保留     
- 枚举方式分区  
- 数字范围方式分区  
- 固定Hash 分区  
- 固定Hash 分区（string 类型）
- 按日期分区

其余分区算法移除  

固定Hash 分区:
特别的sum((count[i]*length[i])) 不受乘积为1024的限制，改为不大于1024的都可以支持，当然，2的次方性能会比较好  
按日期分区:  
可以设置default node  

其余配置见rule_template.xml  

#### 2.2 支持insert 不带columns
启动时拉取db的meta数据，ddl也会修改内存中meta数据，集群部署时DDL需要轮询重启抱枕meta数据更新    

#### 2.3 XA实现分布式(事务MySQL5.7以上) 及recover
具体逻辑：http://10.186.18.11/confluence/pages/viewpage.action?pageId=4327696
目前不完整，待补充     

#### 2.4 移除对其他后端异构数据库的支持,以及对应的配置中的无用项。  
#### 2.5 支持大小写敏感  
需要依赖于mysql结点大小写敏感性以及配置文件一致  
#### 2.6 移除server.xml里一些不必要的配置  
详见http://10.186.18.11/confluence/pages/viewpage.action?pageId=3673209      

#### 2.7 全局表DML(I/U/D)以及DDL的描述和限制  
参见http://10.186.18.11/confluence/pages/viewpage.action?pageId=3671908   
#### 2.8 增加一些其他友好的用户提示  
#### 2.9 全局序列   
移除本地配置和数据库方式配置，默认单机部署采用时间戳方式  
#### 2.10 移除"第一个节点通过数字方式分表"
#### 2.11 其他性能方面的优化   


## 3.已知限制  
#### 3.1 用普通事务两步提交实现的分布式事务的固有问题
第二步commit时有节点出现网络异常会尤为明显    
#### 3.2 多连接并发更新多节点的相同的数据可能引发死锁，并发连接会等待结点超时最终都超时       
并发更新global的同一行数据现象明显  
#### 3.3 由于mysql 不支持事务性ddl，下发时可能导致数据不一致并无法回滚  
  
注： 以上3.1~3.3 暂无一个好的解决方案  
#### 3.4 其他sql的详细支持列表测试中
http://10.186.18.11/confluence/pages/viewpage.action?pageId=3673100
是一个支持列表的相对安全的子集  
#### 3.5 不支持 allowMultiQueries=true  
#### 3.6 不支持view   
#### 3.7 集群部署时，ddl之后需要轮询重启，否则会影响meta数据的正确性  
  
## 4.已知的重要bug(修复中)  
#### 4.1 多次改变autocommit值，事务hang住（偶发） 
#### 4.2 多表查询，多次查询结果不一致 （偶发）  
#### 4.3 set CHARACTER 系列....  

------

 