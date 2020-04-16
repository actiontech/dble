#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from xml.dom.minidom import parse
import xml.dom.minidom
import MySQLdb
import MySQLdb.cursors
from optparse import OptionParser
import logging.config
import decode

# Get manager user infomation.

def getManageruser(serverxml):
    DOMTree = xml.dom.minidom.parse(serverxml)
    collection = DOMTree.documentElement
    if collection.hasAttribute("shelf"):
        print ("Root element : %s" % collection.getAttribute("shelf"))
    manageruser = {"host":"127.0.0.1"}
    system = collection.getElementsByTagName('system')
    propertys = system[0].getElementsByTagName("property")
    for property in propertys:
        if property.getAttribute("name").lower() == "managerport":
            manageruser["port"]=property.childNodes[0].data
    users = collection.getElementsByTagName("user")
    for user in users:
        managerflag = 0
        usingdecrypt = 0
        manageruser["user"] = user.getAttribute("name")
        propertys = user.getElementsByTagName("property")
        for property in propertys:
            if property.getAttribute("name").lower() == "manager" \
                and property.childNodes[0].data.lower() == "true":
                managerflag=1 
            elif property.getAttribute("name").lower() == "usingdecrypt" \
                and int(property.childNodes[0].data) == 1:
                usingdecrypt=1
            elif property.getAttribute("name").lower() == "password":
                manageruser["password"] = property.childNodes[0].data
        if usingdecrypt == 1:
            manageruser["password"] = decode.DecryptByPublicKey(manageruser["password"]).decrypt().split(':')[2]   
        if managerflag:
            return manageruser
    log.error("Don't find manager user in server.xml")

# Get hosts from schema.xml file.

def getHosts(schemaxml):
    DOMTree = xml.dom.minidom.parse(schemaxml)
    collection = DOMTree.documentElement
    if collection.hasAttribute("shelf"):
        print ("Root element : %s" % collection.getAttribute("shelf"))
    datahost_dict = {}
    datahosts = collection.getElementsByTagName("dataHost")
    for datahost in datahosts:
        dh_name = datahost.getAttribute("name")
        hosts_list = []
        writehosts = datahost.getElementsByTagName('writeHost')
        if writehosts:
            for writehost in writehosts:
                writehost_conn = {}
                writehost_conn["dhname"] = dh_name
                writehost_conn["name"] = writehost.getAttribute("host")
                wh_url=writehost.getAttribute("url").split(':')
                writehost_conn["host"] = wh_url[0]
                writehost_conn["port"] = wh_url[1]
                writehost_conn["user"] = writehost.getAttribute("user")
                writehost_conn["password"] = writehost.getAttribute("password")
                usingdecrypt = int(writehost.getAttribute("usingDecrypt"))
                if usingdecrypt == 1:
                    writehost_conn["password"] = \
                    decode.DecryptByPublicKey(writehost_conn["password"]).decrypt().split(':')[3]
                writehost_conn["iswritehost"] = 1
                hosts_list.append(writehost_conn)
        readhosts = writehost.getElementsByTagName('readHost')
        if readhosts:
            for readhost in readhosts:
                readhost_conn = {}
                readhost_conn["dhname"] = dh_name
                readhost_conn["name"] = readhost.getAttribute("host")
                rh_url=readhost.getAttribute("url").split(':')
                readhost_conn["host"] = rh_url[0]
                readhost_conn["port"] = rh_url[1]
                readhost_conn["user"] = readhost.getAttribute("user")
                readhost_conn["password"] = readhost.getAttribute("password")
                usingdecrypt = int(readhost.getAttribute("usingDecrypt"))
                if usingdecrypt == 1:
                    readhost_conn["password"] = \
                    decode.DecryptByPublicKey(readhost_conn["password"]).decrypt().split(':')[3]
                readhost_conn["iswritehost"] = 0
                hosts_list.append(readhost_conn)
        datahost_dict[dh_name]=hosts_list
    return datahost_dict

# If the instance is alive.

def isAlive(datahost):
    conclusion = {}
    try:
        db = MySQLdb.connect(host=datahost["host"],
                             port=int(datahost["port"]),
                             user=datahost["user"],
                             passwd=datahost["password"],
                             cursorclass=MySQLdb.cursors.DictCursor)
        cursor = db.cursor()
        cursor.execute("select * from performance_schema.replication_group_members;")
        result = cursor.fetchall()
        if result:
            conclusion["isalive"] = 1
            conclusion["canbemaster"] = 0
            for row in result:
                if datahost["host"] == row.get("MEMBER_HOST").lower() \
                    and  int(datahost["port"]) == int(row.get("MEMBER_PORT")) \
                    and  row.get("MEMBER_ROLE").lower() == "primary":
                    conclusion["canbemaster"] = 1     
        else:
            conclusion["isalive"] = 1
            conclusion["canbemaster"] = 1
        return conclusion
    except Exception as e:
        log.error("Server {0}:{1}(Null) is dead!" \
            .format(datahost["host"],datahost["port"]))
        log.error("Reason:{0}".format(str(e)))
        conclusion["isalive"] = 0
        conclusion["canbemaster"] = 0
        return conclusion

# Switch datahost master.            

def switchDatahost(manageruser,towritehost):
    switchstage=0
    try:
        db = MySQLdb.connect(host=manageruser["host"],
                             port=int(manageruser["port"]),
                             user=manageruser["user"],
                             passwd=manageruser["password"],
                             cursorclass=MySQLdb.cursors.DictCursor)
        cursor = db.cursor()
        cursor.execute("dataHost @@switch name = '{0}' master = '{1}';" \
            .format(towritehost["dhname"],towritehost["name"])) 
        cursor.execute("show @@datasource;")
        result = cursor.fetchall()
        log.info("Switch Datahost {0} master to {1}!" \
            .format(towritehost["dhname"],towritehost["name"]))
        log.debug(result)
        cursor.close()
        db.close()
        switchstage = 1
    except Exception as e:
        log.error("The action Switch Datahost {0} master to {1} failed!" \
            .format(towritehost["dhname"],towritehost["name"]))
        log.error("Reason:{0}" \
            .format(str(e)))
        cursor.close()
        db.close()
        switchstage = 0
    return switchstage

def main(log1,schemaxml,serverxml):
    global log
    log=log1 
    manager_user = getManageruser(serverxml)

    log.info("Get hosts from schema.xml file.")
    hosts = getHosts(schemaxml)

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
        needswitch = 0
        for host in hosts[dhname]:

            # Writehost is not alive.

            if host["iswritehost"] \
                and not host["isalive"]:
                log.info("Writehost {0}:{1} in {2} is not alive!" \
                    .format(host["host"],host["port"],dhname)) 
                needswitch = 1

            # Writehost is not primary in MGR.

            elif host["iswritehost"] \
                and not host["canbemaster"]:
                log.info("Writehost {0}:{1} in {2} is not primary in MGR!" \
                    .format(host["host"],host["port"],dhname)) 
                needswitch = 1

            # Readhost is not alive.

            elif not host["isalive"]:
                log.info("Instance {0}:{1} in {2} is not alive!" \
                    .format(host["host"],host["port"],dhname))

            # Instace status normal.

            else:
                log.info("Instance {0}:{1} in {2} is normal!" \
                    .format(host["host"],host["port"],dhname))
        if needswitch:
            doswitch = 0
            for host in hosts[dhname]:
                if host["canbemaster"]:
                    log.info("Switch {2} writehost to {0}:{1};due to original writehost is not alive!" \
                        .format(host["host"],host["port"],dhname)) 
                    doswitch = switchDatahost(manager_user,host)
                else:
                    log.info("Do not switch {2} writehost to {0}:{1};due to canbemaster status is 0." \
                        .format(host["host"],host["port"],dhname))
            if doswitch:
                log.info("Switch success!")
            else:
                log.info("Switch failed!")
