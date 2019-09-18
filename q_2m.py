# -*- coding: utf-8 -*-
from elasticsearch import Elasticsearch
import pymysql, requests
import settings

def alert_push_group(s_cluster, s_app, s_appId, s_classname, s_formId,s_opMethod, s_timestamp, s_count):
    url = "http://127.0.0.1:8086/write?db=metrics&u=admin&p=QAZ2wsx3"
    #print(time*10**6)
    payload = 'ERROR_op,clusterName=%s,appName=%s,appId=%s,className=%s,formId=%s,opMethod=%s count=%d %d' % (s_cluster,s_app,s_appId,s_classname,s_formId,s_opMethod,s_count,s_timestamp*10**6)
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
    "size":0,
    "query":{
        "bool":{
            "filter":[
                {
                    "range":{
                        "@timestamp":{
                        "gte":"now-60m","lte":"now","format":"epoch_millis"
                    }
                }},
                {
                    "query_string":{
                        "analyze_wildcard": True,
                        "query":"level:\"ERROR\""}}
                    ]
        }
    },
    "aggs":{
        "6":{
            "terms":{
                "field":"clusterName.keyword","size":500,"order":{"_key":"desc"},"min_doc_count":1
            },
            "aggs":{
                "7":{
                    "terms":{
                        "field":"appName.keyword","size":500,"order":{"_key":"desc"},"min_doc_count":1
                    },
                    "aggs":{
                        "8":{
                            "terms":{
                                "field":"logtags.appId.keyword","size":500,"order":{"_key":"desc"},"min_doc_count":1
                            },
                            "aggs":{
                                "9":{
                                    "terms":{
                                        "field":"className.keyword","size":500,"order":{"_key":"desc"},"min_doc_count":1
                                    },
                                    "aggs":{
                                        "10":{
                                            "terms":{
                                                "field":"logtags.formId.keyword","size":500,"order":{"_key":"desc"},"min_doc_count":1
                                            },
                                            "aggs":{
                                                "3":{
                                                    "terms":{
                                                        "field":"logtags.opMethod.keyword","size":500,"order":{"_key":"desc"},"min_doc_count":1
                                                    },
                                                    "aggs":{
                                                        "2":{
                                                            "date_histogram":{
                                                                "interval":"20s","field":"@timestamp","min_doc_count":0,"extended_bounds":{"min":"now-60m","max":"now"},"format":"epoch_millis"
                                                            },
                                                            "aggs":{}}}}}}}}}}}}}}}}
        print(body)
        response = client.search(
            index="*",
            body=body
        )

        res_list = response.get('aggregations').get('6').get('buckets')
        #print(res_list)

        for cluster in res_list:
            s_cluster = cluster.get('key')
            for app in cluster.get('7').get('buckets'):
                s_app = app.get('key')
                for appId in app.get('8').get('buckets'):
                    s_appId = appId.get('key')
                    for className in appId.get('9').get('buckets'):
                        s_classname = className.get('key')
                        for formId in className.get('10').get('buckets'):
                            s_formId = formId.get('key')
                            for opMethod in formId.get('3').get('buckets'):
                                s_opMethod = opMethod.get('key')
                                loop_count = 0
                                for timestamp in opMethod.get('2').get('buckets'):  
                                    if loop_count == 0:
                                        loop_count += 1
                                        continue
                                    else:
                                        s_timestamp = timestamp.get('key')
                                        s_count = timestamp.get('doc_count')
                                #alert_push_group(s_cluster,s_app,s_classname,self.keyword,s_level,s_count,s_timestamp)
                                #self.update_mysql(s_cluster, s_app, s_classname, s_level, s_timestamp, s_count)
                                print(s_cluster, s_app, s_appId, s_classname, s_formId,s_opMethod, s_timestamp, s_count)
                                alert_push_group(s_cluster, s_app, s_appId, s_classname, s_formId,s_opMethod, s_timestamp, s_count)

if __name__ == "__main__":
    metric1 = Metric(keyword="*")
    metric1.metric_query()

    # metric2 = Metric(keyword="message:\"登录\"")
    # metric2.metric_query()
