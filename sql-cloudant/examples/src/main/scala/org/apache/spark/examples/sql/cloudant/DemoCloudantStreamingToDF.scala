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
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.bahir.cloudant.CloudantReceiver
import org.apache.spark.sql.DataFrame


object DemoCloudantStreamingToDF {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Cloudant Spark SQL External Datasource in Scala")
    // Create the context with a 10 seconds batch size
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val changes = ssc.receiverStream(new CloudantReceiver(sparkConf, Map(
      "database" -> "large-db")))

    // Global RDD
    var globalRDD = ssc.sparkContext.emptyRDD[String]

    changes.foreachRDD((rdd: RDD[String]) => {
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
      }
    })

    ssc.start
    // run streaming for 30 seconds
    ssc.awaitTerminationOrTimeout(30000L)
    ssc.stop()

    // Convert final global RDD[String] to DataFrame
    var finalDataFrame: DataFrame = null

    // Get the singleton instance of SparkSession
    val spark = SparkSessionSingleton.getInstance(globalRDD.sparkContext.getConf)
    finalDataFrame = spark.read.json(globalRDD)
    finalDataFrame.printSchema()

    // Create and cache SQL Temp Table
    finalDataFrame.cache.createOrReplaceTempView("large-db")

    println("FINAL DF COUNT: " + finalDataFrame.count()) // scalastyle:ignore

    val demo = spark.sql(
      s""" SELECT * FROM large-db
        """.stripMargin)
    demo.printSchema()

    println("Temp Table Count: " + demo.count()) // scalastyle:ignore

    finalDataFrame.printSchema()
    finalDataFrame.show(10)
  }
}

