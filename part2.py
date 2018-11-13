#
#Charushi Nanwani charushi 50248736
#Kaushik Panneerselvam kpanneer 50248889
#
from __future__ import print_function
import sys
from operator import add
from pyspark import SparkContext, SQLContext,SparkConf
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import Tokenizer, StringIndexer, IndexToString
from pyspark.ml import Pipeline
from pyspark.ml.classification import NaiveBayes
from pyspark.mllib.util import MLUtils
from pyspark.mllib.evaluation import MulticlassMetrics
# $example off$
from pyspark.sql import SparkSession
import nltk
from nltk.corpus import stopwords
from pyspark.sql.types import *

if __name__ == "__main__":
	spark = SparkSession\
	.builder\
	.appName("Classfication")\
	.getOrCreate()
	conf = SparkConf().setAppName("Classfication")
	conf = (conf.setMaster('local[*]')
		.set('spark.executor.memory', '4G')
		.set('spark.driver.memory', '45G')
		.set('spark.driver.maxResultSize', '10G'))
	spark = SparkSession.builder.master("local").config(conf=conf).getOrCreate()

	path = "/Users/kaushik/Desktop/Lab3/submission/data/"
	path_new_data = path + "new_data/"

	sc = spark.sparkContext
	business_files = sc.wholeTextFiles(path + "b")
	politics_files = sc.wholeTextFiles(path + "p")
	sports_files = sc.wholeTextFiles(path + "s")
	travel_files = sc.wholeTextFiles(path + "t")


	business_rdd = business_files.map(lambda (k,v): ("business",v))
	politics_rdd = politics_files.map(lambda (k,v): ("politics",v))
	sports_rdd = sports_files.map(lambda (k,v): ("sports",v))
	travel_rdd = travel_files.map(lambda (k,v): ("travel",v))

	business_rdd = business_rdd.union(politics_rdd)
	business_rdd = business_rdd.union(sports_rdd)
	business_rdd = business_rdd.union(travel_rdd)

	Data_schema = StructType([
		StructField("category", StringType(), True),
		StructField("content", StringType(), True),
		])
	spark_df = spark.createDataFrame(business_rdd, Data_schema)

	regexTokenizer = RegexTokenizer(inputCol="content", outputCol="words", pattern="\\W")
	# stop words
	add_stopwords = stopwords.words('english')
	add_stopwords.extend(["http","https","amp","rt","t","c","the",'Advertisement','Supported', 'by','Op-Ed', 'Contributor','By','Collapse','SEE', 'MY' ,'OPTIONS','Follow',"Opinion"])
	stopwordsRemover = StopWordsRemover(inputCol="words", outputCol="filtered").setStopWords(add_stopwords)
	label_stringIdx = StringIndexer(inputCol = "category", outputCol = "label")
	
	countVectors = CountVectorizer(inputCol="filtered", outputCol="features", vocabSize=10000, minDF=10)

	pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover, countVectors, label_stringIdx])

	pipelineFit = pipeline.fit(spark_df)
	dataset = pipelineFit.transform(spark_df)
	(trainingData, testData) = dataset.randomSplit([0.8, 0.2], seed = 100)
	
	#Logistic Regression
	lr = LogisticRegression(maxIter=100, regParam=0.01, elasticNetParam=0.01)
	lrModel = lr.fit(trainingData)
	predictions = lrModel.transform(testData)
	predictions.show()

	# Evaluate model results
	evaluator = MulticlassClassificationEvaluator(metricName="f1", labelCol='label')
	predictions_and_labels = predictions.select(["prediction","label"])
	metrics = MulticlassMetrics(predictions_and_labels.rdd)
	conf_mat1 = metrics.confusionMatrix()
	
	precision1 = metrics.precision()
	recall1 = metrics.recall()
	f1Score1 = metrics.fMeasure()


	result = evaluator.evaluate(predictions)

	#Naive Bayes classification
	nb=NaiveBayes(smoothing =2)
	nbModel=nb.fit(trainingData)
	nbPredictions=nbModel.transform(testData)
	nbPredictions.show()

	nbEvaluator=MulticlassClassificationEvaluator(metricName="f1",labelCol='label')

	nbResult = nbEvaluator.evaluate(nbPredictions)

	nb_predictions_and_labels = nbPredictions.select(["prediction","label"])
	
	metrics = MulticlassMetrics(nb_predictions_and_labels.rdd)
	conf_mat2 = metrics.confusionMatrix()
	
	precision2 = metrics.precision()
	recall2 = metrics.recall()
	f1Score2 = metrics.fMeasure()
	
	print("Validation Phase")
	print("------------------------------------------------")
	print("Logistic Regression Accuracy: " + str(result))
	print("Naive Bayes Accuracy: " + str(nbResult))
	print("------------------------------------------------")
	print("Confusion Matrix: Logistic Regression Accuracy: ")
	print(conf_mat1.toArray())
	print("Confusion Matrix: Naive Bayes Accuracy: ")
	print(conf_mat2.toArray())
	print("------------------------------------------------")
	print("Summary Stats for Logistic Regression")
	print("Precision = %s" % precision1)
	print("Recall = %s" % recall1)
	print("F1 Score = %s" % f1Score1)
	print("------------------------------------------------")
	print("Summary Stats for Naive Bayes")
	print("Precision = %s" % precision2)
	print("Recall = %s" % recall2)
	print("F1 Score = %s" % f1Score2)
	print("------------------------------------------------")

	business_new_files = sc.wholeTextFiles(path_new_data + "b")
	politics_new_files = sc.wholeTextFiles(path_new_data + "p")
	sports_new_files = sc.wholeTextFiles(path_new_data + "s")
	travel_new_files = sc.wholeTextFiles(path_new_data + "t")
	business_new_rdd = business_new_files.map(lambda (k,v): ("business",v))
	politics_new_rdd = politics_new_files.map(lambda (k,v): ("politics",v))
	sports_new_rdd = sports_new_files.map(lambda (k,v): ("sports",v))
	travel_new_rdd = travel_new_files.map(lambda (k,v): ("travel",v))

	business_new_rdd = business_new_rdd.union(politics_new_rdd)
	business_new_rdd = business_new_rdd.union(sports_new_rdd)
	business_new_rdd = business_new_rdd.union(travel_new_rdd)

	spark_new_df = spark.createDataFrame(business_new_rdd, Data_schema)

	test_dataset = pipelineFit.transform(spark_new_df)

	idx_to_string = IndexToString(inputCol="prediction", outputCol="category_output",labels=["business","politics","travel","sports"])
	#new_dataset=idx_to_string.transform(test_dataset)
	
	test_predictions = lrModel.transform(test_dataset)
	prediction_with_labels=idx_to_string.transform(test_predictions)

	# Evaluate model results

	test_result = evaluator.evaluate(test_predictions)
	test_predictions_and_labels = test_predictions.select(["prediction","label"])

	test_metrics = MulticlassMetrics(test_predictions_and_labels.rdd)
	test_conf_mat1 = test_metrics.confusionMatrix()
	
	test_precision1 = test_metrics.precision()
	test_recall1 = test_metrics.recall()
	test_f1Score1 = test_metrics.fMeasure()

	test_nbPredictions=nbModel.transform(test_dataset)
	nb_prediction_with_labels=idx_to_string.transform(test_nbPredictions)

	test_nbResult = nbEvaluator.evaluate(test_nbPredictions)
	test_nb_predictions_and_labels = test_nbPredictions.select(["prediction","label"])
	
	test_metrics = MulticlassMetrics(test_nb_predictions_and_labels.rdd)
	test_conf_mat2 = test_metrics.confusionMatrix()
	
	test_precision2 = test_metrics.precision()
	test_recall2 = test_metrics.recall()
	test_f1Score2 = test_metrics.fMeasure()

	test_predictions.show(n=40)

	print("------------------Output for unknown dataset: LogisticRegression-----------------")
	prediction_with_labels.select(["category","category_output"]).show(n=40)

	print("------------------Output for unknown dataset: Naive Bayes-----------------")
	nb_prediction_with_labels.select(["category","category_output"]).show(n=40)


	print("Testing Phase")
	print("------------------------------------------------")
	print("Logistic Regression Accuracy: " + str(test_result))
	print("Naive Bayes Accuracy: " + str(test_nbResult))
	print("------------------------------------------------")
	print("Confusion Matrix: Logistic Regression Accuracy: ")
	print(test_conf_mat1.toArray())
	print("Confusion Matrix: Naive Bayes Accuracy: ")
	print(test_conf_mat2.toArray())
	print("------------------------------------------------")
	print("Summary Stats for Logistic Regression")
	print("Precision = %s" % test_precision1)
	print("Recall = %s" % test_recall1)
	print("F1 Score = %s" % test_f1Score1)
	print("------------------------------------------------")
	print("Summary Stats for Naive Bayes")
	print("Precision = %s" % test_precision2)
	print("Recall = %s" % test_recall2)
	print("F1 Score = %s" % test_f1Score2)
	print("------------------------------------------------")




