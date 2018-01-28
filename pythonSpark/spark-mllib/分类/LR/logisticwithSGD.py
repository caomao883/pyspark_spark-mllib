import numpy as np

from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark import SparkContext,SparkConf
from pyspark.mllib.linalg import Vectors, SparseVector
from pyspark.mllib.regression import LabeledPoint


conf = SparkConf().setMaster("local").setAppName("Test")

sc = SparkContext(conf=conf)
sparse_data = [
 LabeledPoint(0.0, SparseVector(2, {0: 0.0})),
 LabeledPoint(1.0, SparseVector(2, {1: 1.0})),
 LabeledPoint(0.0, SparseVector(2, {0: 1.0})),
 LabeledPoint(1.0, SparseVector(2, {1: 2.0}))
 ]
rdd = sc.parallelize(sparse_data)
model = LogisticRegressionWithSGD.train(rdd,iterations=10)
model.predict(rdd).saveAsTextFile("result/hdfs")
sc.stop()