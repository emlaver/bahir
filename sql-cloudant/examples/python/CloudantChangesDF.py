#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pyspark.sql import SparkSession

# Create Spark session for reading Cloudant data using the _changes endpoint
# Read more about when to consider using the _changes endpoint:
# https://github.com/apache/bahir/blob/master/sql-cloudant/README.md
# Note: use "spark.streaming.unpersist=false" to persist RDDs throughout the load process.
# "cloudant.endpoint" default config is the _all_docs endpoint.
spark = SparkSession\
    .builder\
    .appName("Cloudant Spark SQL Example in Python using dataframes with _changes endpoint")\
    .config("cloudant.host","ACCOUNT.cloudant.com")\
    .config("cloudant.username", "USERNAME")\
    .config("cloudant.password","PASSWORD")\
    .config("cloudant.endpoint", "_changes")\
    .config("spark.streaming.unpersist", "false") \
    .getOrCreate()

# ***1. Loading dataframe from Cloudant db
df = spark.read.load("n_airportcodemapping", "org.apache.bahir.cloudant")
df.printSchema()
df.filter(df.airportName >= 'Moscow').select("_id",'airportName').show()
df.filter(df._id >= 'CAA').select("_id",'airportName').show()


# ***2. Saving a datafram to Cloudant db
df = spark.read.load(format="org.apache.bahir.cloudant", database="n_flight")
df.printSchema()
df2 = df.filter(df.flightSegmentId=='AA106')\
    .select("flightSegmentId", "economyClassBaseCost")
df2.write.save("n_flight2",  "org.apache.bahir.cloudant",
        bulkSize = "100", createDBOnSave="true") 
total = df.filter(df.flightSegmentId >'AA9').select("flightSegmentId", 
        "scheduledDepartureTime").orderBy(df.flightSegmentId).count()
print ("Total", total, "flights from table")


# ***3. Loading dataframe from a Cloudant search index
df = spark.read.load(format="org.apache.bahir.cloudant", database="n_flight", 
        index="_design/view/_search/n_flights")
df.printSchema()
total = df.filter(df.flightSegmentId >'AA9').select("flightSegmentId", 
        "scheduledDepartureTime").orderBy(df.flightSegmentId).count()
print ("Total", total, "flights from index")


# ***4. Loading dataframe from a Cloudant view
df = spark.read.load(format="org.apache.bahir.cloudant", path="n_flight", 
        view="_design/view/_view/AA0", schemaSampleSize="20")
# schema for view will always be: _id, key, value
# where value can be a complex field
df.printSchema()
df.show()
