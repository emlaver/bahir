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
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}

import org.apache.bahir.cloudant.CloudantReceiver


object DemoCloudantStreaming {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Cloudant Spark SQL External Datasource in Scala")
    // Create the context with a 10 seconds batch size
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    val changes = ssc.receiverStream(new CloudantReceiver(sparkConf, Map(
      "database" -> "large-db")))

    // var finalDf: DataFrame = null

    // var i = 0

    var globalRDD = ssc.sparkContext.emptyRDD[String]
    changes.foreachRDD((rdd: RDD[String], time: Time) => {

      println(s"========= $time =========")// scalastyle:ignore

      if (!rdd.isEmpty()) {
        // union RDDs - very efficient operation: https://stackoverflow.com/a/29978189
        globalRDD.union(rdd)
      } else {
        println("FINAL TIME: " + time)// scalastyle:ignore
        ssc.stop(stopSparkContext = true, stopGracefully = false)
      }
    })

    // Get the singleton instance of SparkSession
    val spark = SparkSessionSingleton.getInstance(globalRDD.sparkContext.getConf)

    // Convert final global RDD[String] to DataFrame
    val finalDataFrame = spark.read.json(globalRDD)

    // Create and cache SQL Temp Table
    finalDataFrame.createOrReplaceTempView("demo_medium")
    finalDataFrame.cache.createOrReplaceTempView("demo_medium")

    println("DF Count: " + finalDataFrame.count()) // scalastyle:ignore

    val demo = spark.sql(
      s""" SELECT * FROM demo_medium
        """.stripMargin)
    demo.printSchema()

    println("Temp Table Count: " + demo.count()) // scalastyle:ignore


    ssc.start
    // run streaming until finished
    ssc.awaitTermination
    // ssc.stop(true)
  }
}

