from pyspark import SparkContext, SparkConf
import numpy as np
from operator import add

conf = SparkConf().setMaster("local").setAppName("Test")

sc = SparkContext(conf=conf)

data = np.array([[0, 0, 1], [0, 1, 1], [0, 2, 0], [0, 3, 0],
                 [1, 0, 1], [1, 1, 0], [1, 2, 1], [1, 3, 0],
                 [2, 0, 0], [2, 1, 0], [2, 2, 1], [2, 3, 1],
                 [3, 0, 0], [3, 1, 1], [3, 2, 0], [3, 3, 1],
                 [4, 0, 1], [4, 1, 1], [4, 2, 1], [4, 3, 0],
                 [5, 0, 0], [5, 1, 0], [5, 2, 0], [5, 3, 1]])

rdd = sc.parallelize(data, 2).map(lambda x: (x[0], (x[1], x[2])))
rdd2 = sc.parallelize(data, 2).map(lambda x: (x[0], (x[1], x[2])))
cfrdd1 = rdd.join(rdd2)
resultRdd = cfrdd1.map(lambda x: ((x[1][0][0], x[1][1][0]), 0 if x[1][0][0]==x[1][1][0] else x[1][0][1] & x[1][1][1] ))
# ((ki,kj),vij)
resultRdd = resultRdd.reduceByKey(add)
# ((ki,ki),vii)
filterRdd = resultRdd.filter(lambda x: x[0][0] == x[0][1])
# (1,(ki,vi))
joinrdd = filterRdd.map(lambda x: (1, (x[0][0], x[1])))
rddirddj = joinrdd.join(joinrdd).map(lambda x: ((x[1][0][0], x[1][1][0]), np.sqrt(x[1][0][1] * x[1][1][1])))
# (ki,kj,vi*vj)
resultrdd2 = rddirddj.reduceByKey(add)

resultRdd = resultRdd.join(resultrdd2).map(lambda x: (x[0], x[1][0] / x[1][1])).repartition(1).sortByKey()
#((wi,wj),sim)
resultRdd.saveAsTextFile("result/hdfs1")

userdata = np.array([[2, 0, 0], [2, 1, 0], [2, 2, 1], [2, 3, 1]])

userrdd1 = sc.parallelize(userdata,1)

rdd1 = userdata.map(lambda x:(x[1],(x[0],x[2])))

resultRdd.map(lambda x:(x[0][0],(x[0][1],x[1])))
#(wi,((wj,sim),(u,value)))
rdd2=resultRdd.join(rdd1)

rdd2 = rdd2.map(lambda x:((x[1][1][0],x[1][0][0]),x[1][0][1]*x[1][1][1])).reduceByKey(add)
#(u,wj,recomVlue)
rdd3 = userrdd1.map(lambda x:((x[0],x[1]),x[2]))
#((u,wj),(recomValue,value))
rdd=rdd2.join(rdd3)

rdd.filter(lambda x:x[1][1]==0)






