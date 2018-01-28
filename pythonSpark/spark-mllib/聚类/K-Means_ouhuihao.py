import numpy as np
from pyspark.mllib.linalg import Vector,Vectors

from pyspark.mllib.clustering import KMeans,KMeansModel

from pyspark import SparkContext,SparkConf

conf = SparkConf().setAppName("test").setMaster("local")
sc = SparkContext(conf=conf)


rdd = sc.textFile("data").map(lambda x : [float(v) for v in x.split(" ")])

model = KMeans.train(rdd,2,seed=3)



#模型中心
print(model.centers)
print(model.clusterCenters)
#保存，加载模型
model.save(sc,"result/model/kmean.model")
load_model = KMeansModel.load(sc,"result/model/kmean.model")

#两种预测方式
result=load_model.predict(rdd)
result.saveAsTextFile("result/hdfs")
print(load_model.predict([10.,10.,10.]))
sc.stop()