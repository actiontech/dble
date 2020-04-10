#!/usr/bin/env python3
import time
import subprocess
import logging.config

def log_init(logfile,loggername):
  logging.config.fileConfig(logfile)
  log=logging.getLogger(name=loggername)
  return log

logfile='./logging.conf'
loggername='DBLEDatahostCheck'
log=log_init(logfile,loggername)
log.info("Logger initialization is complete.")
while "true":
    log.info("DBLE datahsots check begin...")
    subprocess.run(['python3', 'dbledatahostcheck.py'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    log.info("DBLE datahsots check end.")
    time.sleep(5)

