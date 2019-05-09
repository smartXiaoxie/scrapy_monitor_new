#coding:utf-8
# 统计每小时的转化率
import redis
import datetime
import time
import mysql.connector

def conn_reids():
    r = redis.Redis(host='127.0.0.1',port=6379)
    return r

def conn_mysql():
    try:
        mysql_client = mysql.connector.connect(host='',user='',passwd='',database='')
        return mysql_client
    except:
        return None

def main():
    day = datetime.datetime.now()
    month = day.strftime('%Y-%m')
    date = day.strftime('%Y-%m-%d %H:%M:%S')
    date = int(time.mktime(time.strptime(date,'%Y-%m-%d %H:%M:%S')))
    redis_client = conn_reids()
    mysql_client = conn_mysql()
    cur = mysql_client.cursor()
    redis_key = "vpn_num:" + str(month)
    redis_sum_key = "vpn_sum:" + str(month)
    if redis_client.exists(redis_key):
        group_list = redis_client.hkeys(redis_key)
        group_dict = {}
        group_sum_dict = {}
        for item in group_list:
            try:
                group_dict[item.split(':')[0]] = int(redis_client.scard(item))
            except:
                pass
            group_sum_dict[item.split(':')[0]] = int(redis_client.hget(redis_key,item))
        if group_dict:
            sql_list = []
            for key,value in group_dict.items():
                item_list = []
                item_list.append(key)
                item_list.append(date)
                used_rate = round(float(group_dict[key]) / float(group_sum_dict[key]),2)
                item_list.append(used_rate)
                item_list.append(int(group_dict[key]))
                item_list.append(int(group_sum_dict[key]))
                item_list = tuple(item_list)
                sql_list.append(item_list)
            # 计算总数选项
            sum_vpn_list = []
            item_sum = redis_client.scard(redis_sum_key)
            sum_vpn = sum(group_sum_dict.values())
            sum_used_rate = round(float(item_sum) / float(sum_vpn),2)
            sum_vpn_list.append("vpn_sum")
            sum_vpn_list.append(date)
            sum_vpn_list.append(sum_used_rate)
            sum_vpn_list.append(item_sum)
            sum_vpn_list.append(sum_vpn)
            sum_vpn_list = tuple(sum_vpn_list)
            sql_list.append(sum_vpn_list)
            # 插入数据库
            insert_sql = "insert into tongji_vpn_all" + "(group_name,check_time,used_rate,collect_sum,sum) VALUES (%s,from_unixtime(%s),%s,%s,%s)"
            try:
                cur.executemany(insert_sql, sql_list)
                mysql_client.commit()
            except:
                print " 插入失败"
        else:
            print "Redis 没有数据"
    mysql_client.close()

if __name__=='__main__':
   main()
