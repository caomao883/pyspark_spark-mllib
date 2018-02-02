from pyspark.mllib.evaluation import RegressionMetrics
from pyspark import SparkContext,SparkConf
conf = SparkConf().setMaster("local").setAppName("Test")
sc = SparkContext(conf=conf)


#R2
predictionAndObservations = sc.parallelize([(2.5, 3.0), (0.0, -0.5), (2.0, 2.0), (8.0, 7.0)])
metrics = RegressionMetrics(predictionAndObservations=predictionAndObservations)
print(metrics.r2)
#RSS
print(metrics.meanAbsoluteError)


