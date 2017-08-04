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

import org.apache.spark.sql.SparkSession

object CloudantSQLTesting {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("Cloudant Spark SQL Example")
      .config("spark.jsonstore.rdd.partitions", 1)
      // .config("spark.cloudant.endpoint", "_changes")
      // .config("spark.streaming.unpersist", "false")
      // .config("cloudant.storageLevel", "MEMORY_ONLY_SER")
      .getOrCreate()

    // For implicit conversions of Dataframe to RDDs
    import spark.implicits._

    // create a temp table from Cloudant db and query it using sql syntax
    spark.sql(
      s"""
         |CREATE TEMPORARY VIEW spark
         |USING org.apache.bahir.cloudant
         |OPTIONS ( database 'spark-500m')
        """.stripMargin)
    // create a dataframe
    val airportData = spark.sql(
      s"""
         |SELECT *
         |FROM spark
        """.stripMargin)
    airportData.printSchema()

  }
}
