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
    manager_user = ''
    users = collection.getElementsByTagName("managerUser")
    for user in users:
        if  not user.getAttribute("readOnly") == "true":
            manager_user = {"host":"127.0.0.1"}
            if user.getAttribute("usingDecrypt") == "true":
                manager_user["user"] = user.getAttribute("name")
                manager_user["password"] = user.getAttribute("password")
                manager_user["password"] = decode.DecryptByPublicKey(manager_user["password"]).decrypt().split(':')[2]
            else:
                manager_user["user"] = user.getAttribute("name")
                manager_user["password"] = user.getAttribute("password")

        if not manager_user:
            log.error("Don't find manager user in xml")
        else:
            return manager_user


# Get manager port from bootstrap.cnf.

def getManagerPort(portCnf):
    b = 9066
    with open(portCnf) as f:
        lines = f.readlines()
        for  line in lines:
            if line.strip().startswith("-DmanagerPort"):
                b=line.split('=')[1].strip('\n')
        return b


# Get dbInstance from db.xml file.

def getDbGroups(dbXml):
    DOMTree = xml.dom.minidom.parse(dbXml)
    collection = DOMTree.documentElement
    if collection.hasAttribute("shelf"):
        print ("Root element : %s" % collection.getAttribute("shelf"))
    dbgroup_dict = {}
    dbgroups  = collection.getElementsByTagName("dbGroup")
    for dbgroup in dbgroups :
        dbgroup_name = dbgroup.getAttribute("name")
        dbinstance_list = []
        dbinstances = dbgroup.getElementsByTagName("dbInstance")
        for dbinstance in dbinstances:
            if  dbinstance.getAttribute("primary") == "true":
                write_dbinstance_conn = {}
                write_dbinstance_conn["dbgroup_name"] = dbgroup_name
                write_dbinstance_conn["name"] = dbinstance.getAttribute("name")
                write_dbinstance_url = dbinstance.getAttribute("url").split(':')
                write_dbinstance_conn["host"] = write_dbinstance_url[0]
                write_dbinstance_conn["port"] = write_dbinstance_url[1]
                write_dbinstance_conn["user"] = dbinstance.getAttribute("user")
                write_dbinstance_conn["password"] = dbinstance.getAttribute("password")
                usingdecrypt = dbinstance.getAttribute("usingDecrypt")
                if usingdecrypt == 'true':
                    write_dbinstance_conn["password"] = \
                    decode.DecryptByPublicKey(write_dbinstance_conn["password"]).decrypt().split(':')[3]
                write_dbinstance_conn["iswritedbinstance"] = 1
                dbinstance_list.append(write_dbinstance_conn)
            else:
                read_dbinstance_conn = {}
                read_dbinstance_conn["dbgroup_name"] = dbgroup_name
                read_dbinstance_conn["name"] = dbinstance.getAttribute("name")
                read_dbinstance_url = dbinstance.getAttribute("url").split(':')
                read_dbinstance_conn["host"] = read_dbinstance_url[0]
                read_dbinstance_conn["port"] = read_dbinstance_url[1]
                read_dbinstance_conn["user"] = dbinstance.getAttribute("user")
                read_dbinstance_conn["password"] = dbinstance.getAttribute("password")
                usingdecrypt = dbinstance.getAttribute("usingDecrypt")
                if usingdecrypt == 'true':
                    read_dbinstance_conn["password"] = \
                    decode.DecryptByPublicKey(read_dbinstance_conn["password"]).decrypt().split(':')[3]
                read_dbinstance_conn["iswritedbinstance"] = 0
                dbinstance_list.append(read_dbinstance_conn)
        dbgroup_dict[dbgroup_name] = dbinstance_list
    return dbgroup_dict


# If the dbInstance is alive.

def isAlive(dbinstance):
    conclusion = {}
    try:
        db = MySQLdb.connect(host = dbinstance["host"],
                             port = int(dbinstance["port"]),
                             user = dbinstance["user"],
                             passwd = dbinstance["password"],
                             cursorclass = MySQLdb.cursors.DictCursor)
        cursor = db.cursor()
        cursor.execute("select @@version as version;")
        mysql_version = cursor.fetchone()["version"]
        if mysql_version:
            conclusion["isalive"] = 1
            conclusion["canbemaster"] = 0
            if '5.7' in mysql_version:
                log.debug("Dbinstance {0}:{1} Mysql Version is {2}." \
                          .format(dbinstance["host"],dbinstance["port"],mysql_version))
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
                log.debug("Dbinstance {0}:{1} Mysql Version is {2}." \
                          .format(dbinstance["host"],dbinstance["port"],mysql_version))
                cursor.execute("select * from performance_schema.replication_group_members;")
                members = cursor.fetchall()
                if members:
                    for row in members:
                        if dbinstance["host"] == row.get("MEMBER_HOST").lower() \
                                and  int(dbinstance["port"]) == int(row.get("MEMBER_PORT")) \
                                and  row.get("MEMBER_ROLE").lower() == "primary":
                            conclusion["canbemaster"] = 1
                else:
                    conclusion["canbemaster"] = 1
            else:
                conclusion["canbemaster"] = 1
                log.Warn("Dbinstance {0}:{1} Mysql Version is not in list '5.7 or 8.0'。" \
                         .format(dbinstance["host"],dbinstance["port"]))
            log.debug("Dbinstance {0}:{1} check done." \
                      .format(dbinstance["host"],dbinstance["port"]))
            return conclusion
    except Exception as e:
        log.error("Dbinstance {0}:{1} is dead!" \
                  .format(dbinstance["host"],dbinstance["port"]))
        log.error("Reason:{0}".format(str(e)))
        conclusion["isalive"] = 0
        conclusion["canbemaster"] = 0
        return conclusion


# Switch dbGroup master.

def switchDbInstance(manager_user,to_write_dbinstance):
    switchStage = 0
    try:
        db = MySQLdb.connect(host = manager_user["host"],
                             port = int(manager_user["port"]),
                             user = manager_user["user"],
                             passwd = manager_user["password"],
                             cursorclass = MySQLdb.cursors.DictCursor)
        log.info("Start Connecting to the Manager[user={0}, host={1}, port={2}]" \
             .format(manager_user["user"],manager_user["host"],manager_user["port"]))
        cursor = db.cursor()
        cursor.execute("dbgroup @@switch name = '{0}' master = '{1}';" \
            .format(to_write_dbinstance["dbgroup_name"],to_write_dbinstance["name"]))
        cursor.execute("show @@dbInstance;")
        result = cursor.fetchall()
        log.info("Switch DbGroup {0} master to {1}!" \
            .format(to_write_dbinstance["dbgroup_name"],to_write_dbinstance["name"]))
        log.debug(result)
        cursor.close()
        db.close()
        switchStage = 1
    except Exception as e:
        log.error("The action Switch DbInstance {0} master to {1} failed!" \
            .format(to_write_dbinstance["dbgroup_name"],to_write_dbinstance["name"]))
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
    port1 = getManagerPort(portCnf)
    manager_user['port'] = port1
    log.info("Get dbGroups from db.xml file.")
    dbGroups = getDbGroups(dbXml)
    log.info("MySQL DbInstance status check.")
    for dbgroup_name in dbGroups.keys():
        log.info("DbGroup {0} check begin!" \
            .format(dbgroup_name))
        for dbinstance in dbGroups[dbgroup_name]:
            isaliveinfo = isAlive(dbinstance)
            dbinstance["isalive"] = isaliveinfo["isalive"]
            dbinstance["canbemaster"] = isaliveinfo["canbemaster"]
        log.info("DbGroup {0} check end." \
            .format(dbgroup_name))

    log.info("Switch check.")
    for dbgroup_name in list(dbGroups.keys()):
        needSwitch = 0
        for dbinstance in dbGroups[dbgroup_name]:

            # Write-dbInstance is not alive.

            if dbinstance["iswritedbinstance"] \
                and not dbinstance["isalive"]:
                log.info("Write-dbInstance {0}:{1} in {2} is not alive!" \
                    .format(dbinstance["host"],dbinstance["port"],dbgroup_name))
                needSwitch = 1

            # Write-dbInstance is not primary in MGR.

            elif dbinstance["iswritedbinstance"] \
                and not dbinstance["canbemaster"]:
                log.info("Write-dbInstance {0}:{1} in {2} is not primary in MGR!" \
                    .format(dbinstance["host"],dbinstance["port"],dbgroup_name))
                needSwitch = 1

            # Readhost is not alive.

            elif not dbinstance["isalive"]:
                log.info("DbInstance {0}:{1} in {2} is not alive!" \
                    .format(dbinstance["host"],dbinstance["port"],dbgroup_name))

            # Instace status normal.

            else:
                log.info("DbInstance {0}:{1} in {2} is normal!" \
                    .format(dbinstance["host"],dbinstance["port"],dbgroup_name))
        if needSwitch:
            doSwitch = ''
            for dbinstance in dbGroups[dbgroup_name]:
                if dbinstance["canbemaster"]:
                    log.info("Switch {2} Write-dbInstance to {0}:{1}; due to original Write-dbInstance is not alive!" \
                        .format(dbinstance["host"],dbinstance["port"],dbgroup_name))
                    doSwitch = switchDbInstance(manager_user,dbinstance)
                else:
                    log.info("Do not switch {2} Write-dbInstance to {0}:{1}; due to canbemaster status is 0." \
                        .format(dbinstance["host"],dbinstance["port"],dbgroup_name))

            if doSwitch:
                log.info("Switch success!")
            else:
                log.info("Switch failed!")
