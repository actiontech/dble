# Quick Start(docker)  
 
## 1. Prepare
 + install docker (in server)
 + install MySQL client tool (server or local)
 + 
## 2. Install
 Run command as below:
```
docker network create -o "com.docker.network.bridge.name"="OBsharding-D-net" --subnet 172.18.0.0/16 OBsharding-D-net
docker run --name backend-mysql1 --network bridge --ip 172.18.0.2  -e MYSQL_ROOT_PASSWORD=123456 -p 33061:3306 --network=OBsharding-D-net -d mysql:5.7 --server-id=1
docker run --name backend-mysql2  --network bridge --ip 172.18.0.3 -e MYSQL_ROOT_PASSWORD=123456 -p 33062:3306 --network=OBsharding-D-net -d mysql:5.7 --server-id=2
sleep 30 # may need more time
docker run -d -i -t --name OBsharding-D-server --ip 172.18.0.5 -p 8066:8066 -p 9066:9066 --network=OBsharding-D-net  actiontech/OBsharding-D:latest
```
 Now we build a docker network. It contains two MySQL containers which map port 3306  to port 33061 and 33062 on the Docker host.   
We map TCP port 8066 and 9066 in the OBsharding-D container to the same port on the Docker host.

`sleep` is waiting for OBsharding-D and MySQL to initialize.
 
## 3. Connect and use
   We have set two users for quick start in docker.  
   8066 port: root/123456    
   9066 port: man1/654321   
   You can view other [configs](https://github.com.oceanbase.obsharding_d/tree/master/docker-images/quick-start) and [table structure](https://github.com.oceanbase.obsharding_d/blob/master/src/main/resources/testdb.sql). 
   
   Try connect MySQL/OBsharding-D by using MySQL client tool. If you want to connet from local,just change the IP.
   ```
   #connect OBsharding-D server port
   mysql -P8066 -u root -p123456 -h 127.0.0.1 
   #connect OBsharding-D manager port
   mysql -P9066 -u man1 -p654321 -h 127.0.0.1
   #connect mysql1
   mysql -P33061 -u root -p123456 -h 127.0.0.1 
   #connect mysql2
   mysql -P33062 -u root -p123456 -h 127.0.0.1
   ```

Enjoy testing.
   
## 4. Clean
clean resources:

```
docker stop backend-mysql1
docker stop backend-mysql2
docker stop OBsharding-D-server
docker rm backend-mysql1
docker rm backend-mysql2
docker rm OBsharding-D-server
docker network rm OBsharding-D-net
```
 
 
