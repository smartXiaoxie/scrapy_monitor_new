# -*- coding: utf-8 -*-
#  这个是切换vpn 使用的 脚本，并发送监控数据到kafka
import sys, os

sys.path.insert(0, os.path.dirname(sys.path[0]))
reload(sys)
sys.setdefaultencoding('utf8')

import re, time
import logging
import datetime
#import urllib
#import urllib2
import json
from kafka import KafkaProducer
import subprocess
import datetime
import os
import signal

os.popen('PATH=$PATH:/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin:~/bin')
os.popen('export PATH')

kill_vpn_pid = "/bin/ps aux|grep pp|grep -v grep|awk '{print $2}' |xargs kill -9"

# 按照网段加入路由
route_cmd = "/sbin/route add -net 0.0.0.0 dev ppp0"

route_find_cmd = "/sbin/route |awk '{print $1}'| grep '%s'"

restore_route_cmd = "/sbin/route add default gw 10.10.0.1 dev eth0"
ip_cmd = "/sbin/ifconfig eth0 | grep -oP '(?<=addr:)[\d.]+'"
vpn_process_cmd = "/bin/ps -ef | grep pptp | grep -v grep | wc -l"


def check_ip():
    result = exe_cmd("/bin/ping -c 3 baidu.com | grep '0 received' | wc -l")
    if result and result.find('0') > -1:
        logging.info("ping is okay")
        return True
    return False


def start_vpn(vpn_id=1):

    vpn_status = exe_cmd('/bin/sh /root/bin/vpn.sh')

    logging.info("vpn_status=%s", vpn_status)

    if vpn_status and vpn_status.find('local  IP address') != -1 and vpn_status.find('Connect: ppp0') != -1 and vpn_status.find(
            'authentication succeeded') != -1:
        logging.debug("add route")
        exe_cmd(route_cmd)
        ip = exe_cmd("curl members.3322.org/dyndns/getip", 120)
        logging.debug("The new vpn ip=%s", ip)
        if ip or check_ip():
            return True
        return False
    else:
        return False


def clean():
    exe_cmd('/usr/sbin/poff', 60)
    if os.popen("ps aux|grep pp|grep -v grep|awk '{print $2}'").read() != '':
        logging.debug("Clean pp related processes ...")
    os.popen(kill_vpn_pid)
    logging.debug("restore route")
    exe_cmd(restore_route_cmd)


def reconnect():
    #day = datetime.date.today()
    #date = day.strftime('%Y-%m-%d')
    pre_time = int(time.time())
    exe_cmd('/usr/sbin/poff', 60)
    if os.popen("ps aux|grep pp|grep -v grep|awk '{print $2}'").read() != '':
        logging.debug("Clean pp related processes ...")
        os.popen(kill_vpn_pid)
        exe_cmd(restore_route_cmd)
    vpn_start = False
    if not start_vpn(1):
        logging.debug("Retrying start_vpn")
        for i in range(2, 4):
            i = int(i)
            logging.debug("Trying times = %d", i)
            if start_vpn(i) == True:
                vpn_start = True
                logging.debug("Reconnected successfully %d", i)
                break
            else:
                logging.debug("Can not start_vpn times=%d", i)
            time.sleep(0.1)
        if not vpn_start:
            logging.debug("Failed to start vpn after %d times retry", i)
            clean()
    else:
        vpn_start = True
        logging.debug("Started ...")
    now_time = int(int(time.time()) - pre_time)
    start_time = datetime.datetime.now()
    date = start_time.strftime("%Y-%m-%d")
    start_time = start_time.strftime("%Y-%m-%d %H:%M:%S")
    messages = {}
    messages["date"] = date
    messages["start_time"] = start_time
    messages["pptp_time"] = now_time
    messages["ip"] = exe_cmd(ip_cmd,20).replace('\n','')
    pptp_process = int(exe_cmd(vpn_process_cmd,20).replace('\n',''))
    if vpn_start == True and pptp_process > 0:
        pptp_status = "True"
    else:
        pptp_status = "False"
    messages["pptp_status"] = pptp_status
    proxy_ip = exe_cmd("curl members.3322.org/dyndns/getip",30)
    if proxy_ip:
        proxy_ip = proxy_ip.replace('\n','')
    else:
        proxy_ip = "0"
    messages["proxy_ip"] = proxy_ip
    
    if os.popen("ps -C squid --no-header | wc -l").read() != 0  and os.popen("find /var/log/squid/ -mtime -2 -name 'access.log' | wc -l").read() !=0:
        squid_status = "True"
    else:
        squid_status = "False"
    messages["squid_status"] = squid_status
    messages["ip_group"] = "ssy_100"
    print messages
    insert_kafka(messages)
    return vpn_start


def exe_cmd(command, timeout=60):
    import subprocess, datetime, os, time, signal
    # cmd = command.split(" ")
    start = datetime.datetime.now()
    process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    while process.poll() is None:
        time.sleep(0.2)
        now = datetime.datetime.now()
        if (now - start).seconds > timeout:
            os.kill(process.pid, signal.SIGKILL)
            os.waitpid(-1, os.WNOHANG)
            return None
    return process.stdout.read()

def conn_kafka():
    try:
       producer = KafkaProducer(bootstrap_servers= ['10.10.183.246:9092','10.10.61.29:9092','10.10.40.200:9092'])
       return producer
    except:
        return None

def insert_kafka(messages):
    kafka_client = conn_kafka()
    if kafka_client:
        message = json.dumps(messages)
        kafka_client.send("mc_scrapy_vpn",message)
    kafka_client.close()

def gen_token(num=32):
    import random
    s = random.sample('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPORSTUVWXYZ1234567890', num)
    tokens = "".join(s)  # generate token.
    return tokens

if __name__ == "__main__":
    reconnect()
    # check_ip()
