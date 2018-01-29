import numpy as np

from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark import SparkContext, SparkConf
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint

conf = SparkConf().setMaster("local").setAppName("Test")

sc = SparkContext(conf=conf)
sparse_data = [
    LabeledPoint(0.0, Vectors.dense([1.0, 0.0])),
    LabeledPoint(1.0, Vectors.dense([0.0, 1.0])),
    LabeledPoint(0.0, Vectors.dense([10.0, 9.0])),
    LabeledPoint(1.0, Vectors.dense([9.0, 10.0]))
]
sparse_data = [
    LabeledPoint(0.0, Vectors.dense([1.0, 0.0])),
    LabeledPoint(1.0, Vectors.dense([0.0, 1.0])),
    LabeledPoint(0.0, Vectors.dense([10.0, 9.0])),
    LabeledPoint(1.0, Vectors.dense([9.0, 10.0]))
]
rdd = sc.parallelize(sparse_data)
model = LogisticRegressionWithSGD.train(rdd, iterations=10)
rdd = rdd.map(lambda x:x.features)
model.predict(rdd).saveAsTextFile("result/hdfs")
sc.stop()
