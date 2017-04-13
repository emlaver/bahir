#*******************************************************************************
# Copyright (c) 2015 IBM Corp.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#******************************************************************************/
from pyspark.sql import SparkSession
from os.path import dirname as dirname
import sys
# add /test to pythonpath so utils can be imported when running from spark
sys.path.append(dirname(dirname(dirname(__file__))))
import helpers.utils as utils
sampleSize = 5

conf = utils.createSparkConf()
#conf.set("sampleSize", sampleSize)

spark = SparkSession\
    .builder\
    .appName("Cloudant Spark SQL Example in Python using dataframes")\
    .config(conf=conf) \
    .getOrCreate()

def verifySampleDataCount():
    print('About to test org.apache.bahir.cloudant for n_customer with setting sampleSize to ' + str(sampleSize))
    customerDataDf = spark.read.load(format="org.apache.bahir.cloudant", path="n_customer", sampleSize=str(sampleSize))
    customerDataDf.cache()
    customerDataDf.printSchema()
    # verify expected count
    print("customerDataDf.count() = ", customerDataDf.count())
    assert customerDataDf.count() == sampleSize


def verifySampleDataCountWithView():
    print('About to test org.apache.bahir.cloudant for n_flight with setting sampleSize to ' + str(sampleSize) + ' and view')
    #flightDataDf = spark.read.load(format="org.apache.bahir.cloudant", path="n_flight",
    #                             view="_design/view/_view/AA0", sampleSize=str(sampleSize))
    flightDataDf = spark.read.load(format="org.apache.bahir.cloudant", path="n_flight", view="_design/view/_view/AA0", sampleSize=str(sampleSize))
    flightDataDf.cache()
    flightDataDf.printSchema()

    # verify expected count
    #print("flightDataDf.count() = ", flightDataDf.count())
    #assert flightDataDf.count() == sampleSize
    print(flightDataDf['doc'])
    print("length: ", flightDataDf.head(5))

    # verify key = "AA0' for the specified sample size
    i = 0
    #flightDataDf.show()
    #flight_info = flightDataDf.filter(flightDataDf("flightSegmentId") >"AA9").count()
    #print("flight count: ", flight_info)
    for flight in flightDataDf.collect():
        print(flight)
        #print('Flight {0} on {1}'.format(flight.key, flight.value))
    #    assert flight.key == 'AA0'
        i += 1
    assert i == flightDataDf.count()


def verifySampleDataWithSampleSchemaSize():
    print('About to test org.apache.bahir.cloudant for n_customer with setting sampleSchemaSize to 2 and sampleSize to ' + str(sampleSize))
    customerDataDf = spark.read.load(format="org.apache.bahir.cloudant", path="n_customer", sampleSize=str(sampleSize),
                                     schemaSampleSize=2)
    customerDataDf.cache()
    customerDataDf.printSchema()

    # verify expected count
    print("customerDataDf.count() = ", customerDataDf.count())
    assert customerDataDf.count() == sampleSize

#verifySampleDataCount()
verifySampleDataCountWithView()
#verifySampleDataWithSampleSchemaSize()
