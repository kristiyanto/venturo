# Scala for recommendation system

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

val path = "hdfs://ec2-52-43-197-246.us-west-2.compute.amazonaws.com:9000/yelp/yelp/yelp_academic_dataset_review.json"
val raw_data = sqlContext.read.json(path)
val raw_data.printSchema()

val data = raw_data.select("user_id", "business_id", "stars")
data.printSchema()

val training_RDD, validation_RDD, test_RDD = data.randomSplit([0.6, 0.2, 0.2], seed=0L)

val Array(training_RDD, validation_RDD, test_RDD) = data.randomSplit(Array(0.6, 0.2, 0.2),seed=0L)
val model = ALS.train(training_RDD, rank, iteration, lambda)

validation_RDD.printSchema()
