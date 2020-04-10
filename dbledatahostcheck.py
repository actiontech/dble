#!/usr/bin/env python3
from xml.dom.minidom import parse
import xml.dom.minidom
import MySQLdb
import MySQLdb.cursors
from optparse import OptionParser
import logging.config

def log_init(logfile,loggername):
  logging.config.fileConfig(logfile)
  log=logging.getLogger(name=loggername)
  return log

#Get hosts from xlmfile.
def getHosts(xmlfile):
    DOMTree = xml.dom.minidom.parse(xmlfile)
    collection = DOMTree.documentElement
    if collection.hasAttribute("shelf"):
        print ("Root element : %s" % collection.getAttribute("shelf"))
    datahost_dict={}
    datahosts = collection.getElementsByTagName("dataHost")
    for datahost in datahosts:
        dh_name=datahost.getAttribute("name")
        hosts_list=[]
        writehosts= datahost.getElementsByTagName('writeHost')
        if writehosts:
            writehost_conn={}
            for writehost in writehosts:
                writehost_conn["dhname"]=dh_name
                writehost_conn["name"]=writehost.getAttribute("host")
                wh_url=writehost.getAttribute("url").split(':')
                writehost_conn["host"]=wh_url[0]
                writehost_conn["port"]=wh_url[1]
                writehost_conn["user"]=writehost.getAttribute("user")
                writehost_conn["password"]=writehost.getAttribute("password")
                writehost_conn["iswritehost"]=1
                hosts_list.append(writehost_conn)
        readhosts= writehost.getElementsByTagName('readHost')
        if readhosts:
            readhost_conn={}
            for readhost in readhosts:
                readhost_conn["dhname"]=dh_name
                readhost_conn["name"]=readhost.getAttribute("host")
                rh_url=readhost.getAttribute("url").split(':')
                readhost_conn["host"]=rh_url[0]
                readhost_conn["port"]=rh_url[1]
                readhost_conn["user"]=readhost.getAttribute("user")
                readhost_conn["password"]=readhost.getAttribute("password")
                readhost_conn["iswritehost"]=0
                hosts_list.append(readhost_conn)
        datahost_dict[dh_name]=hosts_list
    return datahost_dict

#if the instance is alive.
def isAlive(datahost_dict):
    conclusion={}
    try:
        db = MySQLdb.connect(host=datahost_dict["host"],port=int(datahost_dict["port"]),user=datahost_dict["user"],passwd=datahost_dict["password"],cursorclass=MySQLdb.cursors.DictCursor)
        cursor = db.cursor()
        cursor.execute("select * from performance_schema.replication_group_members;")
        result = cursor.fetchall()
        if result:
            conclusion["isalive"]=1
            conclusion["canbemaster"]=0
            for row in result:
                if row.get("MEMBER_ROLE").lower=="primary":
                    conclusion["canbemaster"]=1     
        else:
            conclusion["isalive"]=1
            conclusion["canbemaster"]=1
        return conclusion
    except Exception as e:
        log.error("Server {0}:{1}(Null) is dead!".format(datahost_dict["host"],datahost_dict["port"]))
        log.error("Reason:{0}".format(str(e)))
        conclusion["isalive"]=0
        conclusion["canbemaster"]=0
        return conclusion

#Switch datahost master.            
def switchDatahost(manageruser,towritehost):
    switchstage=0
    try:
        db = MySQLdb.connect(host=manager_user["host"],port=int(manager_user["port"]),user=manager_user["user"],passwd=manager_user["password"],cursorclass=MySQLdb.cursors.DictCursor)
        cursor = db.cursor()
        cursor.execute("dataHost @@switch name = '{0}' master = '{1}';".format(towritehost["dhname"],towritehost["name"])) 
        cursor.execute("show @@datasource;")
        result = cursor.fetchall()
        log.info("Switch Datahost {0} master to {1}!".format(towritehost["dhname"],towritehost["name"]))
        log.info(result)
        cursor.close()
        db.close()
        switchstage=1
    except Exception as e:
        log.error("The action Switch Datahost {0} master to {1} failed!".format(towritehost["dhname"],towritehost["name"]))
        log.error("Reason:{0}".format(str(e)))
        cursor.close()
        db.close()
        switchstage=0
    return switchstage

if __name__ == "__main__":
#logfile initialization
    logfile='./logging.conf'
    loggername='DBLEDatahostCheck'
    log=log_init(logfile,loggername)
##    log.info("Logger initialization is complete.")
#dble schema.xml.
    xmlfile='/opt/dble/conf/schema.xml'
#dble manage user.
    manager_user={'user':'root','password':'root','host':'10.186.64.33','port':9066}

    log.info("Get hosts from xlmfile.")
    hosts=getHosts(xmlfile)

    log.info("MySQL instance status check.")
    for dhname in hosts.keys():
        log.info("Datahost {0} check begin!".format(dhname))
        for i in  range(0, len(hosts[dhname])):
            isalive=isAlive(hosts[dhname][i])
            hosts[dhname][i]["isalive"]=isalive["isalive"]
            hosts[dhname][i]["canbemaster"]=isalive["canbemaster"]
        log.info("Datahost {0} check end.".format(dhname))
   
    log.info("Switch check.")
    for dhname in list(hosts.keys()):
        needswitch=0
        for i in  range(0, len(hosts[dhname])):
            #Writehost is not alive.
            if hosts[dhname][i]["iswritehost"] and not hosts[dhname][i]["isalive"]:
                log.info("Writehost {0}:{1} in {2} is not alive！" .format(hosts[dhname][i]["host"],hosts[dhname][i]["port"],dhname)) 
                needswitch=1
            #Writehost is not primary in MGR.
            elif hosts[dhname][i]["iswritehost"] and not hosts[dhname][i]["canbemaster"]:
                log.info("Writehost {0}:{1} in {2} is not primary in MGR！" .format(hosts[dhname][i]["host"],hosts[dhname][i]["port"],dhname)) 
                needswitch=1
            #Readhost is not alive.
            elif not hosts[dhname][i]["isalive"]:
                log.info("Instance {0}:{1} in {2} is not alive！" .format(hosts[dhname][i]["host"],hosts[dhname][i]["port"],dhname))
            #Instace status normal.
            else:
                log.info("Instance {0}:{1} in {2} is normal!" .format(hosts[dhname][i]["host"],hosts[dhname][i]["port"],dhname))
        if needswitch:
            doswitch=0
            for i in  range(0, len(hosts[dhname])):
                if hosts[dhname][i]["canbemaster"]:
                    log.info("Switch {2} writehost to {0}:{1};due to original writehost is not alive！" .format(hosts[dhname][i]["host"],hosts[dhname][i]["port"],dhname)) 
                    doswitch=switchDatahost(manager_user,hosts[dhname][i])
                else:
                    log.info("Do not switch {2} writehost to {0}:{1};due to canbemaster status is 0." .format(hosts[dhname][i]["host"],hosts[dhname][i]["port"],dhname))
            if doswitch:
                log.info("Switch success!")
            else:
                log.info("Switch failed!")
