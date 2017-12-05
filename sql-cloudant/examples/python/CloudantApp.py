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

spark = SparkSession\
    .builder\
    .appName("Cloudant Spark SQL Example in Python using temp tables")\
    .config("cloudant.host","cloudant.cloudant.com")\
    .config("cloudant.username", "adm-emlaver")\
    .config("cloudant.password","J5fvYEgHiHGSSJVjxMF3M1L99") \
    .config("jsonstore.rdd.partitions", 20)\
    .getOrCreate()

#https://cloudant.cloudant.com/sensu_history_dev/_design/app/_view/by_issued?startkey=[2017,1,1]&endkey=[2017,1,4]
# ***4. Loading dataframe from a Cloudant view
df = spark.read.load(format="org.apache.bahir.cloudant", path="sensu_history_dev",
                     view="_design/app/_view/by_issued", schemaSampleSize="20")
# schema for view will always be: _id, key, value
# where value can be a complex field
df.printSchema()
#df.show()



