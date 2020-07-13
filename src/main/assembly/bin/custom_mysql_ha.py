#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import logging.config
import dble_datahosts_check as Dhcheck

# Need to update.

def init():

    # logfile for initialization

    global logfile
    logfile = './logging.conf'
    global loggername
    loggername = 'DBLEDatahostCheck'

    # dble db.xml.

    global dbxml
    dbxml = '/action/dble/conf/db.xml'

    # dble manage user.

    global userxml
    userxml = '/action/dble/conf/user.xml'
    
    global portcnf
    portcnf = "/action/dble/conf/bootstrap.cnf"

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
        log.info("DBLE datahsots check begin...")
        Dhcheck.main(log,dbxml,userxml,portcnf)
        log.info("DBLE datahsots check end.")
        time.sleep(5)