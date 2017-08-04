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

import java.util.concurrent.BlockingQueue

import com.google.gson.JsonObject
import org.slf4j.{Logger, LoggerFactory}

import org.apache.bahir.cloudant.CloudantChangesConfig

class LoadChanges(config: CloudantChangesConfig, queue: BlockingQueue[JsonObject])
  extends Runnable {
  lazy val logger: Logger = LoggerFactory.getLogger(getClass)
  // Get total number of docs in database using _all_docs endpoint
  lazy val limit: Int = config.getTotal()

  // Testing with no streaming and using blocking queue
  private def loadAll(): Unit = {
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
          queue.add(doc)
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

  override def run(): Unit = {
    loadAll()
  }
}
