# 该项目是新改的scrapy监控流
##环境： python 2.7 + ,需要用到kafka 、Redis、MongoDB，kafka 用来接收scrapy 实时数据流，Redis用于存放中间数据，做任务队列，mongodb 存放最终统计的数据，用于展示的数据源。<br/>

##架构：<br/>
ScrapyMonitorStreaming ——>kafka集群————>消费数据，将数据第一遍聚合传入下一个topic ————>再次消费聚合并将数据格式处理存入mongo,监控任务写入redis ————>tornator 实时从redis 拿任务去mongodb 拿数据统计存成展示使用的格式数据————>django 展示

### 说明：

1.watcher_extension 是嵌套在scrapy 中进行获取数据的，在 setting.py 中进行添加 exrensions，如下：<br/>
EXTENSIONS = {<br/>
   'watcher_extension.SpiderWatcher': 500, <br/>
}<br/>
<br/>
2.main_consumer 第一次消费数据流，进行简单的聚合，减少数据压力，可以多开几个进程，里面采用的是生产者和消费者模式。<br/>
3. consumer_streaming 进行最终消费，处理数据格式，再次聚合，将每次项目的爬取任务写入redis ，并将数据存入mongo，采用生产者和消费者模式。<br/>
4. spider_collection 从redis 接收任务去MongoDB 取数据进行统计，并设置需要检测预警的队列存入redis. 使用tornado 定时，还可以接入其他的模块。 <br/>
5.Monitor_action 是各个监控模块的预警规则，从redis 内接收检测的任务，获取预警项进行判断并推送信息模块，有队推送信息的优化发送策略和推送信息负责人递进的规则，还可以添加更加的规则，采用多进程模式。

6. 最后展示的部分在另一个项目 dsj , 采用
