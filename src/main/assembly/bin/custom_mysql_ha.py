#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import logging.config
import OBsharding-D_dbgroups_check as Dhcheck

# Need to update.

def init():

    # logfile for initialization

    global logfile
    logfile = './bin/custom_mysql_ha_logging.conf'
    global loggername
    loggername = 'OBsharding_DDbGroupsCheck'

    # OBsharding-D db.xml.

    global dbxml
    dbxml = './conf/db.xml'

    # OBsharding-D manage user.

    global userxml
    userxml = './conf/user.xml'

    global portcnf
    portcnf = "./conf/bootstrap.cnf"

# Log file initialization.

def logInit(logfile,loggername):
    logging.config.fileConfig(logfile)
    log = logging.getLogger(name=loggername)
    return log

if __name__ == "__main__":

    # Parameters initialization.

    init()

    log = logInit(logfile,loggername)
    log.info("Logger initialization is complete.")
    while "true":
        log.info("OBsharding-D dbGroups check begin...")
        Dhcheck.main(log,dbxml,userxml,portcnf)
        log.info("OBsharding-D dbGroups check end.")
        time.sleep(5)