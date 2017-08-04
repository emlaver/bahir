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
package org.apache.bahir.cloudant.internal

import scala.collection.mutable.ArrayBuffer

import org.slf4j.{Logger, LoggerFactory}
import play.api.libs.json.Json
import scalaj.http._

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import org.apache.bahir.cloudant.CloudantChangesConfig
import org.apache.bahir.cloudant.common._


class ChangesReceiver(config: CloudantChangesConfig)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK) {

  lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  // Get total number of docs in database using _all_docs endpoint
  lazy val limit: Int = config.getTotal()

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Cloudant Receiver") {
      override def run() { receive() }
    }.start()
  }

  private def receive(): Unit = {
    // Get continuous _changes url
    // val url = config.getChangesReceiverUrl.toString
    val selector: String = {
      "{\"selector\":" + config.getSelector + "}"
    }

    var count = 0

    val changes = config.getDatabase.changes()
      .includeDocs(true).limit(limit)
      .parameter("selector", config.getSelector)
      .continuousChanges()

    // val list = new ArrayBuffer[String]

    while (changes.hasNext) {
      if (count < limit) {
        val feed = changes.next()
        val doc = feed.getDoc
        // Verify that doc is not empty and is not deleted
        if(!doc.has("_deleted")) {
          store(doc.toString)
          // list += doc.toString
          count += 1
        }
        // if (count % config.getStoreInterval == 0) {
        //  store(list)
        // list.clear()
        // }
      } else {
        // exit loop once limit is reached
        return
      }
    }
    changes.stop()
  }
  override def onStop(): Unit = {
  }
}
