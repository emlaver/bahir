/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.sql.cloudant

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

import org.apache.bahir.cloudant.CloudantReceiver


object DemoCloudantStreaming {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setAppName("Cloudant Spark SQL External Datasource in Scala")
    // Create the context with a 10 seconds batch size
    sparkConf.set("spark.streaming.unpersist", "false")
    val ssc = new StreamingContext(sparkConf, Seconds(30))
    // ssc.sparkContext.setLogLevel("DEBUG")

    val changes = ssc.receiverStream(new CloudantReceiver(sparkConf, Map(
      "spark.streaming.unpersist" -> "true",
      "database" -> "large-db")))

    // Global RDD that's created from union of all RDDs
    var globalRDD = ssc.sparkContext.emptyRDD[String]
    // var globalArray = ArrayBuffer("")

    changes.foreachRDD((rdd: RDD[String], time: Time) => {
      // Convert final global RDD[String] to DataFrame
      var finalDataFrame: DataFrame = null

      // Persist RDD for later use: https://forums.databricks.com/answers/272/view.html
      rdd.persist(StorageLevel.MEMORY_AND_DISK)

      if (!rdd.isEmpty()) {
        if (globalRDD != null) {
          // Union each RDD in foreach loop
          // val r = rdd.collect()
          globalRDD = globalRDD.union(rdd)
        } else {
          globalRDD = rdd
          // globalArray = rdd.collect()
        }
      } else {
        // Get the singleton instance of SparkSession
        val spark = SparkSessionSingleton.getInstance(globalRDD.sparkContext.getConf)

        println("FINAL TIME: " + time)// scalastyle:ignore

        // Get the singleton instance of SparkSession
        // val spark = SparkSessionSingleton.getInstance(globalRDD.sparkContext.getConf)
        finalDataFrame = spark.read.json(globalRDD)
        finalDataFrame.printSchema()

        // Create and cache SQL Temp Table
        finalDataFrame.createOrReplaceTempView("demo_medium")
        finalDataFrame.cache.createOrReplaceTempView("demo_medium")

        println("FINAL DF COUNT: " + finalDataFrame.count()) // scalastyle:ignore

        val demo = spark.sql(
          s""" SELECT * FROM demo_medium
        """.stripMargin)
        demo.printSchema()

        println("Temp Table Count: " + demo.count()) // scalastyle:ignore

        ssc.stop(stopSparkContext = true, stopGracefully = false)
        // ssc.stop()
      }
    })


    ssc.start
    // run streaming until finished
    ssc.awaitTermination
    // ssc.stop(stopSparkContext = true, stopGracefully = false)
  }
}

