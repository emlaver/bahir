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

import org.joda.time.{DateTime, Seconds}

import org.apache.spark.sql.SparkSession

object DemoReadChangesPerformanceTest {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Cloudant Spark SQL Example")
      .getOrCreate()

    // For implicit conversions of Dataframe to RDDs
    import spark.implicits._

    // scalastyle:off println
    val start = DateTime.now()
    println(s"Starting test: $start")
    // scalastyle:on println

    val df = spark.read.format("org.apache.bahir.cloudant").load("large-db")
    // Caching df in memory to speed computations
    // and not to retrieve data from cloudant again
    df.cache()
    // df.persist()
    // df.printSchema()

    // val fiteredDf = df.filter(df("gender") === "male" && df("age") <= 50)
    // .select("greeting", "tags")

    // println(s"Total # of rows in largeTable: " + fiteredDf.count()) // scalastyle:ignore

    // scalastyle:off println
    val end = DateTime.now()
    println(s"Ending test: $end")
    val duration = Seconds.secondsBetween(start, end)// Minutes.minutesBetween(start, end)
    println(s"---------- DONE: ${duration.getSeconds} ----------")
    // scalastyle:on println
  }
}
