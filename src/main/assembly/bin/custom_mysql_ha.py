#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import logging.config
import dble_datahosts_check as Dhcheck

# Need to update.

def init():

    # logfile for initialization

    global logfile
    logfile = './bin/custom_mysql_ha_logging.conf'
    global loggername
    loggername = 'DBLEDatahostCheck'

    # dble sharding.xml.

    global schemaxml
    schemaxml = './conf/sharding.xml'

    # dble manage user.

    global serverxml
    serverxml = './conf/server.xml'

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
        Dhcheck.main(log,schemaxml,serverxml)
        log.info("DBLE datahsots check end.")
        time.sleep(5)


