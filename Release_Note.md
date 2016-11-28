
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

###2.支持insert 不带columns
