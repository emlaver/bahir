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

package org.apache.bahir.cloudant

import java.io.File

import org.apache.spark.sql.{SQLContext, SQLImplicits, SparkSession}
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnitSuite



class CloudantAppSuite extends JUnitSuite { self =>
  private val tempDir: File = new File(System.getProperty("java.io.tmpdir") + "/sql-cloudant/")

  //import spark implicits: http://stackoverflow.com/questions/39151189/importing-spark-implicits-in-scala
  var spark: SparkSession = _

  private object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = self.spark.sqlContext
  }
  import testImplicits._

  @Before def before() {
    tempDir.mkdirs()
    tempDir.deleteOnExit()
    val spark = SparkSession
      .builder()
      .appName("Cloudant Spark SQL Example")
      .config("cloudant.host", System.getenv("CLOUDANT_HOST")) // i.e. CLOUDANT_HOST = https://ACCOUNT.cloudant.com
      .config("cloudant.username", System.getenv("CLOUDANT_USER"))
      .config("cloudant.password", System.getenv("CLOUDANT_PASSWORD"))
      .getOrCreate()
  }

  @After def after() {
    TestUtils.deleteRecursively(tempDir)
  }

  @Test def totalRowsInAirportData() {

    // create a temp table from Cloudant db and query it using sql syntax
    spark.sql(
      s"""
         |CREATE
         |TEMPORARY TABLE airportTable
         |USING org.apache.bahir.cloudant
         |OPTIONS ( database 'n_airportcodemapping')
        """.stripMargin)
    // create a dataframe
    val airportData = spark.sql(
      s"""
         |SELECT _id, airportName
         |FROM airportTable
         |WHERE _id >= 'CAA' AND _id <= 'GAA' ORDER BY _id
        """.stripMargin)
    airportData.printSchema()
    println(s"Total # of rows in airportData: " + airportData.count()) // scalastyle:ignore
    assert(airportData.count() == 13)

    //verify >= 'CAA' ORDER BY _id
    val previous_id = ""
    for (code <-  airportData.collect()) {
      //code.get("_id")
      println(code.get(0))
      println(code.get(1))
      println(code.get(2))
      println(code.get(3))
    }

    // convert dataframe to array of Rows, and process each row
    airportData.map(t => "code: " + t(0) + ",name:" + t(1)).collect().foreach(println) // scalastyle:ignore
  }

  @Test def totalRowsInFlightTable() {
    // create a temp table from Cloudant index  and query it using sql syntax
    spark.sql(
      s"""
         |CREATE TEMPORARY TABLE flightTable
         |USING org.apache.bahir.cloudant
         |OPTIONS (database 'n_flight', index '_design/view/_search/n_flights')
        """.stripMargin)
    val flightData = spark.sql(
      s"""
         |SELECT flightSegmentId, scheduledDepartureTime
         |FROM flightTable
         |WHERE flightSegmentId >'AA9' AND flightSegmentId<'AA95'
        """.stripMargin)
    flightData.printSchema()
    flightData.map(t => "flightSegmentId: " + t(0) + ", scheduledDepartureTime: " + t(1))
      .collect().foreach(println) // scalastyle:ignore
  }
}
