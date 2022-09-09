
from operator import concat
import re
import sys
import requests
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F



def aggregate_count(new_values, total_sum):
	  return sum(new_values) + (total_sum or 0)
def aggregate_text(new,previuos):
    #ensures this stream will run from the start of our application
    return concat(new,previuos or [])

def get_sql_context_instance(spark_context):
    if('sqlContextSingletonInstance' not in globals()): 
        globals()['sqlContextSingletonInstance'] = SparkSession(spark_context)
    return globals()['sqlContextSingletonInstance']

    #sends data to flask webapp at port 5000 at updateData_languageCount
def send_df_to_dashboard_1(df):
    url = 'http://webapp:5000/updateData_languageCount'
    data = df.toPandas().to_dict('list')
    requests.post(url, json=data)
    
    #sends data to flask webapp at port 5000 at updateData_pushCount
def send_df_to_dashboard_2(df):
    url = 'http://webapp:5000/updateData_pushCount'
    data = df.toPandas().to_dict('list')
    requests.post(url, json=data)
    
    #sends data to flask webapp at port 5000 at updateData_starCount
def send_df_to_dashboard_3(df):
    url = 'http://webapp:5000/updateData_starCount'
    data = df.toPandas().to_dict('list')
    requests.post(url, json=data)
    
    #sends data to flask webapp at port 5000 at updateData_wordCount
def send_df_to_dashboard_4(df):
    url = 'http://webapp:5000/updateData_wordCount'
    data = df.toPandas().to_dict('list')
    requests.post(url, json=data)

# we group by languange and count repository.all repository are alreday distinct here, also adds a time column, prints and sends data
def process_rdd_1(time, rdd):
    pass
    print("Question 1----------- %s -----------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        results_df = sql_context.createDataFrame(rdd,["Language","repository","count"])
        results_df=results_df.groupBy("Language").agg(F.count("repository").alias('Total_count'),F.lit(str(time)).alias('time'))
        results_df.show()                                                                                                     
        send_df_to_dashboard_1(results_df)
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)
        
#creates dataframe,counts distinct repositories, adds time column, prints and sends data to dashboard associated to q2
def process_rdd_2(time, rdd):
    pass
    print("Question2----------- %s -----------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        results_df = sql_context.createDataFrame(rdd,["Language","repository","count"])
        results_df_2=results_df.groupBy("Language").agg(F.countDistinct("repository").alias('Push_count'),F.lit(str(time)).alias('time'))
        results_df_2.show()                                                       
        send_df_to_dashboard_2(results_df_2)
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)
        
#sets as dataframe, groups, and we get the average star for each repository by grouping by language, print and then send to dashboard associated to q3
def process_rdd_3(time, rdd):
    pass
    print("Question 3----------- %s -----------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        results_df = sql_context.createDataFrame(rdd,["Language","repository","star"])
        results_df_3=results_df.groupBy("Language").agg(F.avg("star").alias('Average_star'),F.lit(str(time)).alias('time'))
        results_df_3.show()                                         
        send_df_to_dashboard_3(results_df_3)
    except ValueError:
        print("Waiting for data...")
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)

#sets as a data dataframe
## column word contain list like [[],[]] so we need to explode this two times so that we can count and group by language
#Sends to dashboard associated with q4      
def process_rdd_4(time, rdd):
    pass
    print("Question 4----------- %s -----------" % str(time))
    try:
        sql_context = get_sql_context_instance(rdd.context)
        results_df = sql_context.createDataFrame(rdd,["Language","repository","word"])
        results_df=results_df.select("Language", F.explode("word").alias("word")) 
        results_df4=results_df.select("Language", F.explode("word").alias("word"))\
        .groupBy("Language", "word").count()\
        .orderBy("count",ascending=False).select("Language", "word","count",F.lit(str(time)).alias('time'))
        results_df4.show(50)
        send_df_to_dashboard_4(results_df4)
    except ValueError:
        print("Waiting for data...")
    except: 
        e = sys.exc_info()[0]
        print("Error: %s" % e)

if __name__ == "__main__":
    DATA_SOURCE_IP = "data-source"
    DATA_SOURCE_PORT = 9999
    sc = SparkContext(appName="gitApp")
    sc.setLogLevel("ERROR")
    ssc = StreamingContext(sc,60) #creates a stream that functions every 1 minute
    ssc.checkpoint("checkpoint_gitApp")
    data = ssc.socketTextStream(DATA_SOURCE_IP, DATA_SOURCE_PORT)
    data=data.flatMap(lambda string: string.split("\n")).map(lambda x:x.split("\t")) ## according to data source , we split first with repo delimiter and second with var delimiter to get each fields
    data_1=data.map(lambda ksv: ((ksv[0], ksv[1]),1 )).reduceByKey(lambda x,y:x+y) #ksv[0]= language ksv[1]= full_name of repo. So we will reduce by language and repo and we count 
    aggregated_counts_key = data_1.updateStateByKey(aggregate_count) ### here we aggregate each result for each batch inteval
    aggregated_counts=aggregated_counts_key.map(lambda x: (list(x[0])[0],list(x[0])[1],x[1])) ## we change the format so that we can map while creating a dataframe result in process_rdd
    aggregated_counts.foreachRDD(process_rdd_1) ## for each rdd we apply process_rdd to create our result 
    data_2=data_1.map(lambda x: (list(x[0])[0],list(x[0])[1],x[1]))
    data_2.foreachRDD(process_rdd_2)
    data_3_aggregated=data.map(lambda ksv: ((ksv[0], ksv[1]),int(ksv[2]) )).reduceByKey(lambda x, y: x+y).updateStateByKey(aggregate_count) ### ksv[2]=number star we count it for each repository and culculate result
    data_3_aggregated=data_3_aggregated.join(aggregated_counts_key).mapValues(lambda x: x[0] / x[1]) ### we divide by the total of repository for each language and repository
    data_3_aggregated=data_3_aggregated.map(lambda x: (list(x[0])[0],list(x[0])[1],x[1])) ## we change the format our result
    data_3_aggregated.foreachRDD(process_rdd_3)
    ### code below will filter None value on description and we group by language and repository . we conserve only one description for each repo
    data_4=data.filter(lambda ksv:ksv[3]!="None").map(lambda ksv: ((ksv[0], ksv[1]),ksv[3])).reduceByKey(lambda x,y:re.sub('[^a-zA-Z ]', '', y)) ## we apply a re transforamtion to split space and retrieve only word
    data_4=data_4.map(lambda x:(x[0],x[1].split(" "))).updateStateByKey(aggregate_text) ## we aggregate our word list for each batch interval
    data_4=data_4.map(lambda x: (list(x[0])[0],list(x[0])[1],x[1])) ## we change the format
    data_4.foreachRDD(process_rdd_4)
    
    
    ssc.start()
    ssc.awaitTermination()
