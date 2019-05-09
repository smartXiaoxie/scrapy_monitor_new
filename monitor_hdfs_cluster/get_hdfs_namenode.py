#coding:utf-8
# 获取 hdfs 集群的数据存入数据库，并并行监控预警，待完善，集群内部组件一下堆使用和rpc 时间监控还未加入
# 现里面有存两种数据库，mysql 用于grafana, mongo 用于自主开发的平台，等平台完善可停用grafana
import requests
import ast
import sys,os,signal
import time
import mysql.connector
import redis
from kafka import KafkaProducer
from pymongo import MongoClient
import subprocess
import datetime
reload(sys)
sys.setdefaultencoding('utf8')


class conn_db(object):
    def __init__(self):
        pass
    def conn_mysql(self):
        try:
            conn = mysql.connector.connect(host='',port=,user='',passwd='',database='')
            return conn
        except:
            return None

    def conn_kafka(self):
        try:
            producer = KafkaProducer(bootstrap_servers=['10.10.183.246:9092', '10.10.61.29:9092', '10.10.40.200:9092'])
            return producer
        except:
            return None

    def conn_mongo(self):
        try:
            conn_mongo_client = MongoClient('mongodb://10.10.92.98:27017').streaming
            return conn_mongo_client
        except:
            return None

    def conn_redis(self):
        try:
            conn = redis.Redis(host='10.10.73.23',port=6379)
            return conn
        except:
            return None

class MonitorYarn(object):
    def __init__(self,ip,port,cluster_id,active_name):
        self.ip = ip
        self.port = port
        self.cluster_id = cluster_id
        self.active_name = active_name
        self.timeout = 60

    def GetYarnInfo(self):
        url = "http://" + str(self.ip) + ':' + str(self.port) + '/ws/v1/cluster/metrics'
        response = requests.get(url)
        if response.status_code != 200:
            return None
        yarn_dict = {}
        r = response.json()
        yarn_dict["cluster_id"] = self.cluster_id
        yarn_dict["active_name"] = self.active_name
        yarn_dict["total_mem"] = r["clusterMetrics"]["totalMB"] * 1024
        yarn_dict["total_cpu"] = r["clusterMetrics"]["totalVirtualCores"]
        yarn_dict["total_live"] = r["clusterMetrics"]["activeNodes"]
        yarn_dict["unhealthyNodes"] = r["clusterMetrics"]["unhealthyNodes"]
        yarn_dict["used_mem"] = r["clusterMetrics"]["allocatedMB"] * 1024
        yarn_dict["used_cpu"] = r["clusterMetrics"]["appsRunning"]
        yarn_dict["appsPending"] = r["clusterMetrics"]["appsPending"]
        yarn_dict["appsRuning"] = r["clusterMetrics"]["appsRunning"]
        print yarn_dict
        return yarn_dict

class MonitorHdfs(object):
    def __init__(self,ip,port,cluster_id,active_name):
        self.ip = ip
        self.port = port
        self.cluster_id = cluster_id
        self.active_name = active_name
        self.timeout = 120

    def get_hdfs_Cluster_State(self):
        url = "http://" + str(self.ip) + ':' + str(self.port) + '/jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus'
        response =  requests.get(url)
        if response.status_code != 200:
            return  None
        r = response.json()
        cluster_status = str(r["beans"][0]["State"])

    def get_hdfs_Cluster_info(self):
        Cluster = {}
        url = "http://" + str(self.ip) + ':' + str(self.port) + '/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState'
        response = requests.get(url,self.timeout)
        if response.status_code != 200:
            return None
        print type(response)
        r = response.json()
        hdfs_total = r["beans"][0]["CapacityTotal"]
        hdfs_used = r["beans"][0]["CapacityUsed"]
        hdfs_remaining = r["beans"][0]["CapacityRemaining"]
        live_node = r["beans"][0]["NumLiveDataNodes"]
        dead_node = r["beans"][0]["NumDeadDataNodes"]
        file_total = r["beans"][0]["FilesTotal"]
        blocks_total = r["beans"][0]["BlocksTotal"]
        Cluster["hdfs_total"] = int(hdfs_total)
        Cluster["hdfs_used"] = int(hdfs_used)
        Cluster["hdfs_remaining"] = int(hdfs_remaining)
        Cluster["live_node"] = live_node
        Cluster["dead_node"] = dead_node
        Cluster["file_total"] = file_total
        Cluster["blocks_total"] = blocks_total
        Cluster["active_name"] = self.active_name
        Cluster["cluster_id"] = self.cluster_id
        return Cluster

    def get_hdfs_data_node(self):
        cluster1_dict = {}
        node_list = []
        url = "http://" + str(self.ip) + ':' + str(self.port) + '/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo'
        response = requests.get(url,self.timeout)
        if response.status_code != 200:
            return None
        r = response.json()
        cluster1_dict["version"] = str(r["beans"][0]["Version"].split(',')[0])
        cluster1_dict["miss_block"] = r["beans"][0]["NumberOfMissingBlocks"]
        node_dict = r["beans"][0]["LiveNodes"]
        node_dict = str(node_dict)
        node_dict = ast.literal_eval(node_dict)
        for key,value in node_dict.items():
            node_info = {}
            node_info["node_name"] = key
            node_ip = value["xferaddr"].split(':')[0]
            node_info["node_ip"] = node_ip
            node_info["node_state"] = value["adminState"]
            node_info["node_size_total"] = value["capacity"]
            node_info["node_size_used"] = value["used"]
            node_info["non_dfs_used"] = value["nonDfsUsedSpace"]
            node_info["node_size_remaining"] = value["remaining"]
            node_info["block_num"] = value["numBlocks"]
            node_info["cluster_id"] = self.cluster_id
            node_list.append(node_info)
        return  cluster1_dict,node_list

    
def get_data(cmd,timeout=120):
    start_time = datetime.datetime.now()
    process = subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    while process.poll() is None:
        time.sleep(2)
        now_time = datetime.datetime.now()
        if (now_time - start_time).seconds > timeout:
            os.kill(process.pid,signal.SIGKILL)
            os.waitpid(-1,os.WNOHANG)
            return None
    else:
        return process.stdout.read()


if __name__ == '__main__':
    hdfs_cluster = {"master":"10.10.165.74","master-standby":"10.10.108.62"}
    yarn_cluster = {"rm1":"10.10.165.74","rm2":"10.10.108.62"}
    hdfs_cmd = "/usr/local/hadoop/bin/hdfs haadmin -getServiceState master"
    hdfs_data = get_data(hdfs_cmd)
    hdfs_data = hdfs_data.replace('\n','')
    if hdfs_data == "active":
        ip = hdfs_cluster["master"]
        active_name = ip
    else:
        ip = hdfs_cluster["master-standby"]
        active_name = ip
    conn = conn_db()
    client = conn.conn_mysql()
    cur = client.cursor()
    redis_client = conn.conn_redis()
    # 连接 mongo
    mongo_client = conn.conn_mongo()
    redis_list = []
    port = "50070"
    cluster_id = "hadoop1"
    monitor = MonitorHdfs(ip, port, cluster_id, active_name)
    # 插入数据库,先是HdfsClusterinfo
    get_cluster_dict = monitor.get_hdfs_Cluster_info()
    cluster_state = monitor.get_hdfs_Cluster_State()
    get_cluster_dict1,node_list = monitor.get_hdfs_data_node()
    t_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    get_cluster_dictMerged = {}
    # 插入cluster_id 数据
    if get_cluster_dict and get_cluster_dict1:
        get_cluster_dict["cluster_status"] = cluster_state
        get_cluster_dictMerged = dict(get_cluster_dict,**get_cluster_dict1)
        cluster_sql = "insert into HdfsClusterinfo (cluster_id,active_name,cluster_status,hdfs_total,hdfs_used,hdfs_remaining," \
                      "live_node,dead_node,file_total,blocks_total,version,t_time,miss_block) VALUES (" + "'" + get_cluster_dictMerged["cluster_id"] + "',"\
                      + "'" + get_cluster_dictMerged["active_name"] + "'," + "'" + get_cluster_dictMerged["cluster_status"] + "'," + str(get_cluster_dictMerged["hdfs_total"]) + "," \
                      + str(get_cluster_dictMerged["hdfs_used"]) + "," + str(get_cluster_dictMerged["hdfs_remaining"]) + ","+ str(get_cluster_dictMerged["live_node"])+ "," \
                      + str(get_cluster_dictMerged["dead_node"]) + "," + str(get_cluster_dictMerged["file_total"]) + "," + str(get_cluster_dictMerged["blocks_total"]) + "," + "'" \
                      + get_cluster_dictMerged["version"] + "'," + "'" + str(t_time) + "'," + str(get_cluster_dictMerged["miss_block"]) + ")"
        print cluster_sql
        try:
            cur.execute(cluster_sql)
            client.commit()
            redis_list.append("HdfsClusterinfo")
        except:
            print " 插入失败"

        # 插入mongodb 集合 HdfsClusterinfo

        print get_cluster_dictMerged
        get_cluster_dictMerged["t_time"] = t_time
        try:
            mongo_client.HdfsClusterinfo.insert(get_cluster_dictMerged)
            if "HdfsClusterinfo" not in redis_list:
                redis_list.append("HdfsClusterinfo")
        except:
            print " 插入失败"
        mongo_client.Bigdata_deplay.remove({"table_name":"HdfsClusterinfo"})
        get_cluster_dictMerged["table_name"] = "HdfsClusterinfo"
        mongo_client.Bigdata_deplay.insert(get_cluster_dictMerged)
    else:
        hdfs_cluste_state = "off"
        cluster_sql = "insert into HdfsClusterinfo (cluster_id,active_name,cluster_status,hdfs_total,hdfs_used,hdfs_remaining,live_node," \
                     "dead_node,file_total,blocks_total,version,t_time,miss_block) VALUES (" + "'" + str(monitor.cluster_id) + "'," + "'"\
                     + str(monitor.active_name) + "'," + "'" + str(hdfs_cluste_state) + "'," + "0,0,0,0,0,0,0," + "'" + 0 + "'," + "'" + str(t_time) + \
                     "'," + "0)"
        print  cluster_sql
        try:
            cur.execute(cluster_sql)
            client.commit()
            redis_list.append("HdfsClusterinfo")
        except:
            print " 插入失败"
        # 插入到mongo
        try:
            mongo_client.HdfsClusterinfo.insert({"cluster_id":monitor.cluster_id,"active_name":monitor.active_name,"cluster_status":hdfs_cluste_state,
                                                 "hdfs_total":0,"hdfs_used":0,"hdfs_remaining":0,"live_node":0,"dead_node":0,"file_total":0,"blocks_total":0,
                                                 "Version":'0',"t_time":t_time,"miss_block":0})
            if "HdfsClusterinfo" not in redis_list:
                redis_list.append("HdfsClusterinfo")
        except:
            print "插入失败"
        mongo_client.Bigdata_deplay.remove({"table_name":"HdfsClusterinfo"})
        mongo_client.Bigdata_deplay.insert({"cluster_id":monitor.cluster_id,"active_name":monitor.active_name,"cluster_status":hdfs_cluste_state,
                                            "hdfs_total": 0, "hdfs_used": 0, "hdfs_remaining": 0, "live_node": 0,"dead_node": 0,"file_total":0,"blocks_total":0,
                                            "Version": '0', "t_time": t_time, "miss_block": 0,"table_name":"HdfsClusterinfo"})
    # 插入 node 数据
    if node_list:
        data = []
        mongo_data = []
        for item in node_list:
            data_list = []
            item["t_time"] = t_time
            mongo_data.append(item)
            data_list.append('')
            data_list.append(item["node_name"])
            data_list.append(item["node_ip"])
            data_list.append(item["node_state"])
            data_list.append(item["node_size_total"])
            data_list.append(item["node_size_used"])
            data_list.append(item["non_dfs_used"])
            data_list.append(item["node_size_remaining"])
            data_list.append(item["block_num"])
            data_list.append(item["t_time"])
            data_list.append(item["cluster_id"])
            data_list = tuple(data_list)
            data.append(data_list)
        data = tuple(data)
        data_sql = "insert into HdfsNodeinfo" + "(id,node_name,node_ip,node_state,node_size_total,node_size_used,non_dfs_used,node_size_remaining,block_num,t_time,cluster_id) VALUES " \
                                            "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        try:
            cur.executemany(data_sql, data)
            client.commit()
            redis_list.append("HdfsNodeinfo")
        except:
            print "插入失败"

        # 插入mongo 数据库
        mongo_client.Bigdata_deplay.remove({"table_name":"HdfsNodeinfo"})
        for mongo_item in mongo_data:
            mongo_client.HdfsNodeinfo.insert(mongo_item)
            mongo_item["table_name"] = "HdfsNodeinfo"
            mongo_client.Bigdata_deplay.insert(mongo_item)
    else:
        print "没有数据，不用插入"

    # 插入yarn 的数据
    yarn_cmd = "/usr/local/hadoop/bin/yarn rmadmin -getServiceState rm1"
    yarn_data = get_data(yarn_cmd)
    yarn_data = yarn_data.replace('\n','')
    if yarn_data == "active":
        yarn_ip = yarn_cluster["rm1"]
        yarn_active_name = yarn_ip
    else:
        yarn_ip = yarn_cluster["rm2"]
        yarn_active_name = yarn_ip
    yarn_port = "8088"
    yarn_cluster_id = "yarn1"
    monitor_yarn = MonitorYarn(yarn_ip, yarn_port, yarn_cluster_id, yarn_active_name)
    yarn_dict = monitor_yarn.GetYarnInfo()
    if yarn_dict:
        yarn_dict["t_time"] = t_time
        yarn_cluster_state = "alived"
        yarn_dict["cluster_state"] = yarn_cluster_state
        yarn_sql = "insert into YarnClusterinfo (cluster_id,active_name,total_mem,total_cpu,total_live,unhealthyNodes,used_mem,used_cpu,t_time,cluster_state,appsPending,appsRuning) VALUES (" + "'" + yarn_dict["cluster_id"] + "'," + "'" + yarn_dict["active_name"] +"'," + str(yarn_dict["total_mem"]) + "," + str(yarn_dict["total_cpu"]) + "," + str(yarn_dict["total_live"]) + "," + str(yarn_dict["unhealthyNodes"]) +"," + str(yarn_dict["used_mem"]) +"," + str(yarn_dict["used_cpu"]) + "," + "'" + str(t_time) + "'," + "'" + str(yarn_cluster_state) + "'," + str(yarn_dict["appsPending"]) + "," + str(yarn_dict["appsRuning"]) + ")"
        print yarn_sql
        try:
            cur.execute(yarn_sql)
            client.commit()
            redis_list.append("YarnClusterinfo")
        except:
            print " 插入失败"

        # 插入mongodb 数据库
        try:
            mongo_client.YarnClusterinfo.insert(yarn_dict)
            if "YarnClusterinfo" not in redis_list:
                redis_list.append("YarnClusterinfo")
        except:
            print " 插入失败"
        mongo_client.Bigdata_deplay.remove({"table_name":"YarnClusterinfo"})
        yarn_dict["table_name"] = "YarnClusterinfo"
        mongo_client.Bigdata_deplay.insert(yarn_dict)
    else:
        yarn_cluster_state = "off"
        yarn_sql = "insert into YarnClusterinfo (cluster_id,active_name,total_mem,total_cpn,total_live,unhealthyNodes,used_mem,used_cpu,t_time,cluster_state) VALUES ("
        + "'" + str(monitor_yarn.cluster_id) + "'," + "'" + str(monitor_yarn.active_name) + "'," + "0,0,0,0,0,0," + "'" + str(t_time) + "'," + "'" + str(yarn_cluster_state) + "')"
        print yarn_sql
        try:
            cur.execute(yarn_sql)
            client.commit()
            redis_list.append("YarnClusterinfo")
        except:
             print " 插入失败"
        # 插入mongo 数据库
        try:
            mongo_client.YarnClusterinfo.insert({"cluster_id":monitor.cluster_id,"active_name":monitor.active_name,"total_mem":0,"total_cpu":0,"total_live":0,
                                                 "unhealthyNodes":0,"used_mem":0,"used_cpu": 0,"t_time": t_time,"cluster_state": yarn_cluster_state})
            if "YarnClusterinfo" not in redis_list:
                redis_list.append("YarnClusterinfo")
        except:
            print " 插入失败"
        mongo_client.Bigdata_deplay.remove({"table_nam":"YarnClusterinfo"})
        mongo_client.Bigdata_deplay.insert({"cluster_id":monitor.cluster_id,"active_name":monitor.active_name,"total_mem":0,"total_mem":0,"total_cpu":0,"total_live":0,
                                            "unhealthyNodes": 0, "used_mem": 0, "used_cpu": 0, "t_time": t_time,"cluster_state":yarn_cluster_state,"table_name":"YarnClusterinfo"})

    # 插入redis  后进先出的原理
    if redis_list:
        len_queue = redis_client.llen("hdfs_queue")
        if len_queue > 5:
            diff_i = len_queue - 5
            for i in range(diff_i):
                redis_key = redis_client.lpop("hdfs_queue")
                redis_client.delete(redis_key)
        queue_key = "hdfs" + "+" + str(t_time.split()[0]) + '_' + str(t_time.split()[1])
        print queue_key
        for  value in redis_list:
             redis_client.sadd(queue_key,value)
        redis_client.rpush("hdfs_queue",queue_key)


