# Quick Start
## 1.Download dble Release 
Get package from https://github.com/actiontech/dble/releases

## 2.Prepare
### 2.1 MySQL 
Make sure there is at least one MySQL Instance with url $url(e.g., localhost:3306) ,$user(e.g., test) and $password（e.g., testPsw） in your machine.  
Add 4 database ,the SQL as below: 
 
```
create database db1;  
create database db2;  
create database db3;  
create database db4;
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

create table travelrecord(
id int,
name char(255) ,
PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

show full tables;
```


## 4.Stop

```   

cd $working_dir/dble
./bin/dble stop

```


