# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch
import pymysql, requests
import settings

def alert_push_group(clusterName,appName,className,keyword,level,count,time):
    url = "http://127.0.0.1:8086/write?db=metrics&u=admin&p=QAZ2wsx3"
    #print(time*10**6)
    payload = '%s,clusterName=%s,appName=%s,className=%s,keyword=%s count=%d %d' % (level,clusterName,appName,className,keyword,count,time*10**6)
    response = requests.post(url, data=payload.encode('utf-8'))
    print(response.status_code)
    #print(response.headers)


class Metric(object):
    def __init__(self,**kwargs):
        self.interval = settings.interval
        self.eshost = settings.eshost
        self.dbconfig = settings.dbconfig
        for k,v in kwargs.items():
            setattr(self,k,v)

    def update_mysql(self,clusterName,appName,classname, level,timestamp,doc_count):
        # 打开数据库连接
        db = pymysql.connect(host=self.dbconfig['host'], user=self.dbconfig['user'], passwd=self.dbconfig['password'],
                             db=self.dbconfig['db'], charset='utf8', port=self.dbconfig['port'])

        # 使用cursor()方法获取操作游标
        cursor = db.cursor()
        # SQL 更新语句
        sql = "insert into %s (clusterName, appName, className, level, keyword, timestamp, doc_count) values (\"%s\", \"%s\", \"%s\", \"%s\",\"%s\",from_unixtime(%d div 1000 ), \"%d\")" % (
        self.dbconfig['table'], clusterName,appName,classname, level,pymysql.escape_string(self.keyword),timestamp,doc_count)
        #print(sql)
        try:
            # 执行SQL语句
            cursor.execute(sql)
            # 提交到数据库执行
            db.commit()
        except:
            # 发生错误时回滚
            db.rollback()
        # 关闭数据库连接
        db.close()


    def metric_query(self):
        client = Elasticsearch(self.eshost)
        body = {
            "query": {
                "bool": {
                    "filter": [
                        {"range": {"@timestamp": {"gte": "now-2m", "lte": "now"}}},
                        {"query_string": {
                            "analyze_wildcard": True,
                            "query": "%s" % self.keyword
                        }}
                    ]
                }
            },
            "aggs": {
                "5": {
                    "terms": {
                        "field": "clusterName.keyword",
                        "size": 500,
                        "order": {
                            "_term": "desc"
                        },
                        "min_doc_count": 1
                    },
                    "aggs": {
                        "6": {
                            "terms": {
                                "field": "appName.keyword",
                                "size": 500,
                                "order": {
                                    "_term": "desc"
                                },
                                "min_doc_count": 1
                            },
                            "aggs": {
                                "7": {
                                    "terms": {
                                        "field": "className.keyword",
                                        "size": 500,
                                        "order": {
                                            "_term": "desc"
                                        },
                                        "min_doc_count": 1
                                    },
                                    "aggs": {
                                        "3": {
                                            "terms": {
                                                "field": "level.keyword",
                                                "size": 500,
                                                "order": {
                                                    "_term": "desc"
                                                },
                                                "min_doc_count": 1
                                            },
                                            "aggs": {
                                                "2": {
                                                    "date_histogram": {
                                                        "interval": "%s" % self.interval,
                                                        "field": "@timestamp",
                                                        "min_doc_count": 0,
                                                        "extended_bounds": {
                                                            "min": "now-2m",
                                                            "max": "now"
                                                        },
                                                        "format": "epoch_millis"
                                                    },
                                                    "aggs": { }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        print(body)
        response = client.search(
            index="*",
            body=body
        )

        res_list = response.get('aggregations').get('5').get('buckets')
        print(res_list)

        for cluster in res_list:
            s_cluster = cluster.get('key')
            print(s_cluster)
            for app in cluster.get('6').get('buckets'):
                s_app = app.get('key')
                for classname in app.get('7').get('buckets'):
                    s_classname = classname.get('key')
                    for level in classname.get('3').get('buckets'):
                        s_level = level.get('key')
                        loop_count = 0
                        for timestamp in level.get('2').get('buckets'):
                            if loop_count == 0:
                                loop_count += 1
                                continue
                            else:
                                s_timestamp = timestamp.get('key')
                                s_count = timestamp.get('doc_count')
                                #alert_push_group(s_cluster,s_app,s_classname,self.keyword,s_level,s_count,s_timestamp)
                                #self.update_mysql(s_cluster, s_app, s_classname, s_level, s_timestamp, s_count)
                                #print(s_cluster, s_app, s_classname, s_level, s_timestamp, s_count)
#so what now

if __name__ == "__main__":
    metric1 = Metric(keyword="*")
    metric1.metric_query()

    # metric2 = Metric(keyword="message:\"登录\"")
    # metric2.metric_query()