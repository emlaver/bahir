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
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
#from pyspark.streaming.kafka import KafkaUtils
from os.path import dirname as dirname
import sys
# add /test to pythonpath so utils can be imported when running from spark
sys.path.append(dirname(dirname(dirname(__file__))))
import helpers.utils as utils
sampleSize = 5

conf = utils.createSparkConf()
conf.set("sampleSize", sampleSize)

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 1)

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 9999)

#spark = SparkSession\
#    .builder\
#    .appName("Cloudant Spark SQL Example in Python using streaming")\
#    .config(conf=conf) \
#    .getOrCreate()

