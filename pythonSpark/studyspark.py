from pyspark import SparkConf,SparkContext
import numpy as np
a = np.array([1,2,4,5,3,4,5,6])
import py4j
#file = open('../result/result.txt','w')

def func(x):
    print(x)

def calSum(x,y):
    #file.write(str(x+y)+"\n")
    return x+y

def deel(x):
    return (1,x)
conf = SparkConf().setMaster('local').setAppName("MyApp")
sc = SparkContext(conf=conf)
rdd = sc.parallelize(a,1).map(lambda x:(x,1)).groupByKey().saveAsTextFile("result/hdfs3")


#file.close()