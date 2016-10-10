/** Scala for recommendation system */

import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating

val path = "hdfs://ec2-52-43-197-246.us-west-2.compute.amazonaws.com:9000/yelp/yelp/yelp_academic_dataset_review.json"
val raw_data = sqlContext.read.json(path)
raw_data.printSchema()

val data = raw_data.select("user_id", "business_id", "stars")
data.printSchema()

val Array(training_RDD, validation_RDD, test_RDD) = data.randomSplit(Array(0.6, 0.2, 0.2),seed=0L)

validation_RDD.printSchema()

val train_RDD = training_RDD.map(x => (x.getAs[String]("user_id").zipWithIndex.foreach{case(user_id, count) => count},x.getAs[String]("business_id"),x.getAs[Int]("Stars")))

val model = ALS.train(train_RDD, rank, iteration, lambda)

from pyspark.ml.recommendation import ALS

val als = ALS(maxIter=5, regParam=0.01, userCol="user_id", itemCol="business_id", ratingCol="stars")
model = als.fit(training_RDD)