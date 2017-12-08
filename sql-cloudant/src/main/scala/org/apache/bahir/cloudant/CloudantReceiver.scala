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

import java.io.InputStreamReader

import play.api.libs.json.Json

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import org.apache.bahir.cloudant.common._

class CloudantReceiver(sparkConf: SparkConf, cloudantParams: Map[String, String])
    extends Receiver[String](StorageLevel.MEMORY_AND_DISK) {
  // CloudantChangesConfig requires `_changes` endpoint option
  lazy val config: CloudantChangesConfig = {
    JsonStoreConfigManager.getConfig(sparkConf, cloudantParams
      + ("cloudant.endpoint" -> JsonStoreConfigManager.CHANGES_INDEX)
    ).asInstanceOf[CloudantChangesConfig]
  }

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Cloudant Receiver") {
      override def run() { receive() }
    }.start()
  }

  private def receive(): Unit = {
    val url = config.getContinuousChangesUrl.toString
    val selector: String = if (config.getSelector != null) {
      "{\"selector\":" + config.getSelector + "}"
    } else {
      null
    }

    val changesRequest = config.executeRequest(url, selector)
    if (changesRequest.getConnection.getResponseCode / 100 == 2 ) {
      import java.io.BufferedReader
      val reader = new BufferedReader(
        new InputStreamReader(changesRequest.responseAsInputStream()))
      var count = 0
      var line = ""
      while ((line = reader.readLine) != null) {
        if (line.length() > 0) {
          val json = Json.parse(line)
          val jsonDoc = (json \ "doc").getOrElse(null)
          var doc = ""
          if (jsonDoc != null) {
            doc = Json.stringify(jsonDoc)
            // Verify that doc is not empty and is not deleted
            val deleted = (jsonDoc \ "_deleted").getOrElse(null)
            if (!doc.isEmpty && deleted == null) {
              store(doc)
              count += 1
            }
          }
        }
      }
    } else {
      // TODO
      // val status = headers.getOrElse("Status", IndexedSeq.empty)
      // val status = changesRequest.responseInterceptors.getOrElse("Status", IndexedSeq.empty)
      val errorMsg = "Error retrieving _changes feed " + config.getDbname + ": " // + status(0)
      reportError(errorMsg, new CloudantException(errorMsg))
    }
  }

  def onStop(): Unit = {
  }
}
