# Quick Start
## 1.Download dble Release 
Get package from https://github.com/actiontech/dble/releases

## 2.Prepare
### 2.1 MySQL 
Make sure there is at least two MySQL Instance with url $url(e.g., localhost:3306) ,$user(e.g., test) and $password（e.g., testPsw） in your machine.   
You also need to make sure that the url(localhost/127.0.0.1/other IP) can connect to MySQL, otherwise,you will get an error "NO ROUTE TO HOST" later. So Check your configurations of “/etc/hosts” ,“/etc/hosts.allow” ,“/etc/hosts.deny”  
Add 6 database ,the SQL as below: 
 
instance1:
```  
create database db_1;  
create database db_3;  
create database db_5;  
```  
instance2:
```  
create database db_2;  
create database db_4;  
create database db_6;  
```  

### 2.2 JVM 
Make sure JAVA version is 1.8 and JAVA_HOME has been set.The older version may occurs Exception.

## 3.Install

```   

mkdir -p $working_dir  
cd $working_dir  
tar -xvf actiontech-dble-$version.tar.gz  
cd $working_dir/dble/conf  
cp rule_template.xml rule.xml  
cp schema_template.xml schema.xml  
cp server_template.xml server.xml  

```

## 4.Config
Edit the file schema.xml.  
Find the dataHost element, delete all the writeHost/readHost element below.
Create a new writeHost element like 
```  
  <writeHost host="hostM1" url="$url" user="$user" password="$password"/>
```  
(replace to your own MySQL information)  
the other writehost also need to config  
Save the schema.xml  


 
## 4.Start  

start cmd:  

```  
$working_dir/dble/bin/dble start

```  


check log in $working_dir/logs

```   
tail -f logs/wrapper.log 
```

You should see "Server startup successfully. see logs in logs/dble.log".

## 5.connect
As a distributed-database imitate mysql,you can use all Mysql classic connection.  
In this case you can connect to the dble using command:
```
mysql -p -P8066 -h 127.0.0.1 -u root
```  
Enter the password 123456 to login in
```
use testdb;
drop table if exists tb_enum_sharding;
create table if not exists tb_enum_sharding (
  id int not null,
  code int not null,
  content varchar(250) not null,
  primary key(id)
)engine=innodb charset=utf8;
insert into tb_enum_sharding values(1,10000,'1'),(2,10010,'2'),(3,10000,'3'),(4,10010,'4');

show full tables;
```


## 4.Stop

```   

cd $working_dir/dble
./bin/dble stop

```


