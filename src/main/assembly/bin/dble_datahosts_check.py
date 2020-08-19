#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import MySQLdb
import MySQLdb.cursors
import xml.dom.minidom
import logging.config
import decode

# Get manager user infomation.


def getMangagerUser(userXml):
    DOMTree = xml.dom.minidom.parse(userXml)
    collection = DOMTree.documentElement
    if collection.hasAttribute("shelf"):
        print ("Root element : %s" % collection.getAttribute("shelf"))
    managerUser = ''
    users = collection.getElementsByTagName("managerUser")
    for user in users:
        if  not user.getAttribute("readOnly") == "true":
            managerUser = {"host":"127.0.0.1"}        
            if user.getAttribute("usingDecrypt") == "true":  
                managerUser["user"] = user.getAttribute("name")
                managerUser["password"] = user.getAttribute("password")
                managerUser["password"] = decode.DecryptByPublicKey(managerUser["password"]).decrypt().split(':')[2]     
            else:
                managerUser["user"] = user.getAttribute("name")
                managerUser["password"] = user.getAttribute("password")
       
        if not managerUser:
            log.error("Don't find manager user in xml")
        else:
            return managerUser
    
          
# Get manager port from bootstrap.cnf.

def getPort(portCnf):
    b = 9066
    with open(portCnf) as f:
        lines = f.readlines()
        for  line in lines:
            if line.strip().startswith("-DmanagerPort"):
                b=line.split('=')[1].strip('\n')
        return b
                
# Get dbInstance from db.xml file.


def getHosts(dbXml):
    DOMTree = xml.dom.minidom.parse(dbXml)
    collection = DOMTree.documentElement
    if collection.hasAttribute("shelf"):
        print ("Root element : %s" % collection.getAttribute("shelf"))
    dbgroup_dict = {}
    dbgroups  = collection.getElementsByTagName("dbGroup")
    for dbgroup in dbgroups :
        dh_name = dbgroup.getAttribute("name")
        hosts_list = []
        dbinstances = dbgroup.getElementsByTagName("dbInstance")
        for dbinstance in dbinstances:
            if  dbinstance.getAttribute("primary") == "true":
                writehost_conn = {}
                writehost_conn["dhname"] = dh_name
                writehost_conn["name"] = dbinstance.getAttribute("name")
                wh_url = dbinstance.getAttribute("url").split(':')
                writehost_conn["host"] = wh_url[0]
                writehost_conn["port"] = wh_url[1]
                writehost_conn["user"] = dbinstance.getAttribute("user")
                writehost_conn["password"] = dbinstance.getAttribute("password")
                usingdecrypt = dbinstance.getAttribute("usingDecrypt")
                if usingdecrypt == 'true':
                    writehost_conn["password"] = \
                    decode.DecryptByPublicKey(writehost_conn["password"]).decrypt().split(':')[3]
                writehost_conn["iswritehost"] = 1
                hosts_list.append(writehost_conn)
            else:
                readhost_conn = {}
                readhost_conn["dhname"] = dh_name
                readhost_conn["name"] = dbinstance.getAttribute("name")
                rh_url = dbinstance.getAttribute("url").split(':')
                readhost_conn["host"] = rh_url[0]
                readhost_conn["port"] = rh_url[1]
                readhost_conn["user"] = dbinstance.getAttribute("user")
                readhost_conn["password"] = dbinstance.getAttribute("password")
                usingdecrypt = dbinstance.getAttribute("usingDecrypt")
                if usingdecrypt == 'true':
                    readhost_conn["password"] = \
                    decode.DecryptByPublicKey(readhost_conn["password"]).decrypt().split(':')[3]
                readhost_conn["iswritehost"] = 0
                hosts_list.append(readhost_conn)
        dbgroup_dict[dh_name] = hosts_list
    return dbgroup_dict
        
    
# If the instance is alive.

def isAlive(dbgroup):
    conclusion = {}
    try:
        db = MySQLdb.connect(host = dbgroup["host"],
                             port = int(dbgroup["port"]),
                             user = dbgroup["user"],
                             passwd = dbgroup["password"],
                             cursorclass = MySQLdb.cursors.DictCursor)
        cursor = db.cursor()
        cursor.execute("select @@version as version;")
        mysql_version = cursor.fetchone()["version"]
        if mysql_version:
            conclusion["isalive"] = 1
            conclusion["canbemaster"] = 0
            if '5.7' in mysql_version:
                log.debug("Server {0}:{1} version is {2}." \
                          .format(dbgroup["host"],dbgroup["port"],mysql_version))
                cursor.execute("select * from performance_schema.replication_group_members;")
                members = cursor.fetchall()
                if members:
                    cursor.execute("select @@group_replication_single_primary_mode as single_primary_mode;")
                    single_primary_mode = cursor.fetchone()["single_primary_mode"]
                    if single_primary_mode == 1:
                        cursor.execute("show status like 'group_replication_primary_member';")
                        primary_member = cursor.fetchone()["Value"]
                        cursor.execute("select @@server_uuid as server_uuid;")
                        server_uuid = cursor.fetchone()["server_uuid"]
                        if primary_member == server_uuid:
                            conclusion["canbemaster"] = 1
                else:
                    conclusion["canbemaster"] = 1
            elif '8.0' in mysql_version:
                log.debug("Server {0}:{1} version is {2}." \
                          .format(dbgroup["host"],dbgroup["port"],mysql_version))
                cursor.execute("select * from performance_schema.replication_group_members;")
                members = cursor.fetchall()
                if members:
                    for row in members:
                        if dbgroup["host"] == row.get("MEMBER_HOST").lower() \
                                and  int(dbgroup["port"]) == int(row.get("MEMBER_PORT")) \
                                and  row.get("MEMBER_ROLE").lower() == "primary":
                            conclusion["canbemaster"] = 1
                else:
                    conclusion["canbemaster"] = 1
            else:
                conclusion["canbemaster"] = 1
                log.Warn("The server {0}:{1} version is not in list '5.7 or 8.0'。" \
                         .format(dbgroup["host"],dbgroup["port"]))
            log.debug("Server {0}:{1} check done." \
                      .format(dbgroup["host"],dbgroup["port"]))
            return conclusion
    except Exception as e:
        log.error("Server {0}:{1} is dead!" \
                  .format(dbgroup["host"],dbgroup["port"]))
        log.error("Reason:{0}".format(str(e)))
        conclusion["isalive"] = 0
        conclusion["canbemaster"] = 0
        return conclusion

# Switch dbgroup master.            

def switchDatahost(manageruser,towritehost):
    switchStage = 0
    try:
        db = MySQLdb.connect(host = manageruser["host"],
                             port = int(manageruser["port"]),
                             user = manageruser["user"],
                             passwd = manageruser["password"],
                             cursorclass = MySQLdb.cursors.DictCursor)
        log.info("Start Connecting to the Manager[user={0}, host={1}, port={2}]" \
             .format(manageruser["user"],manageruser["host"],manageruser["port"]))
        cursor = db.cursor()
        cursor.execute("dbgroup @@switch name = '{0}' master = '{1}';" \
            .format(towritehost["dhname"],towritehost["name"])) 
        cursor.execute("show @@dbInstance;")
        result = cursor.fetchall()
        log.info("Switch Datahost {0} master to {1}!" \
            .format(towritehost["dhname"],towritehost["name"]))
        log.debug(result)
        cursor.close()
        db.close()
        switchStage = 1
    except Exception as e:
        log.error("The action Switch Datahost {0} master to {1} failed!" \
            .format(towritehost["dhname"],towritehost["name"]))
        log.error("Reason:{0}" \
            .format(str(e)))
        cursor.close()
        db.close()
        switchStage = 0
    return switchStage
    
def main(log1,dbXml,userXml,portCnf):
    global log
    log = log1 
    manager_user = getMangagerUser(userXml)
    port1 = getPort(portCnf)
    manager_user['port'] = port1
    log.info("Get hosts from db.xml file.")
    hosts = getHosts(dbXml)
    log.info("MySQL instance status check.")
    for dhname in hosts.keys():
        log.info("Datahost {0} check begin!" \
            .format(dhname))
        for host in hosts[dhname]:
            isalive = isAlive(host)
            host["isalive"] = isalive["isalive"]
            host["canbemaster"] = isalive["canbemaster"]
        log.info("Datahost {0} check end." \
            .format(dhname))
            
    log.info("Switch check.")
    for dhname in list(hosts.keys()):
        needSwitch = 0
        for host in hosts[dhname]:

            # Writehost is not alive.

            if host["iswritehost"] \
                and not host["isalive"]:
                log.info("Writehost {0}:{1} in {2} is not alive!" \
                    .format(host["host"],host["port"],dhname)) 
                needSwitch = 1

            # Writehost is not primary in MGR.

            elif host["iswritehost"] \
                and not host["canbemaster"]:
                log.info("Writehost {0}:{1} in {2} is not primary in MGR!" \
                    .format(host["host"],host["port"],dhname)) 
                needSwitch = 1

            # Readhost is not alive.

            elif not host["isalive"]:
                log.info("Instance {0}:{1} in {2} is not alive!" \
                    .format(host["host"],host["port"],dhname))

            # Instace status normal.

            else:
                log.info("Instance {0}:{1} in {2} is normal!" \
                    .format(host["host"],host["port"],dhname))
        if needSwitch:
            doSwitch = ''
            for host in hosts[dhname]:
                if host["canbemaster"]:
                    log.info("Switch {2} writehost to {0}:{1};due to original writehost is not alive!" \
                        .format(host["host"],host["port"],dhname)) 
                    doSwitch = switchDatahost(manager_user,host)
                else:
                    log.info("Do not switch {2} writehost to {0}:{1};due to canbemaster status is 0." \
                        .format(host["host"],host["port"],dhname))
            
            if doSwitch:
                log.info("Switch success!")
            else:
                log.info("Switch failed!")
    