# Quick Start(docker-compose)  
## 1. Prepare
 + install docker (in server)
 + install docker-compose (in server)
 + install MySQL client tool (server or local)
## 2. Install
 Download [docker-compose.yml](https://raw.githubusercontent.com.oceanbase.obsharding_d/master/docker-images/quick-start/docker-compose.yml) 
Run command as below: 
```
docker-compose up
```

Now we build a docker network. It contains two MySQL containers which map port 3306  to port 33061 and 33062 on the Docker host.   
We map TCP port 8066 and 9066 in the OBsharding-D container to the same port on the Docker host. 
 
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
   mysql -P9066 -u man1 -p123456 -h 127.0.0.1
   #connect mysql1
   mysql -P33061 -u root -p123456 -h 127.0.0.1 
   #connect mysql2
   mysql -P33062 -u root -p123456 -h 127.0.0.1
   ```

Enjoy testing.
   
   
 
### 4. Clean

Clean resources:
```
docker-compose stop
```
or

```
docker-compose down
```
 
