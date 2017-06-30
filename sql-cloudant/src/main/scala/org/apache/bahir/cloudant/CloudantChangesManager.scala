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

import java.util.concurrent.BlockingQueue

import org.slf4j.{Logger, LoggerFactory}
import scalaj.http.{Http, HttpRequest}

import org.apache.bahir.cloudant.common.CloudantException

class CloudantChangesManager {}

  // Concrete producer
  class ChangesFeedProducer[T](url: String, config: CloudantChangesConfig,
                    queue: BlockingQueue[String], limit: Int)
    extends Runnable {
    lazy val logger: Logger = LoggerFactory.getLogger(getClass)
    var isThreadStopped: Boolean = false

    override def run() {
      val requestTimeout = config.requestTimeout.toInt
      val selector: String = if (config.getSelector != null) {
        "{\"selector\":" + config.getSelector + "}"
      } else {
        "{}"
      }

      val clRequest: HttpRequest = config.username match {
        case null =>
          Http(url)
            .postData(selector)
            .timeout(connTimeoutMs = 1000, readTimeoutMs = requestTimeout)
            .header("Content-Type", "application/json")
            .header("User-Agent", "spark-cloudant")
        case _ =>
          Http(url)
            .postData(selector)
            .timeout(connTimeoutMs = 1000, readTimeoutMs = requestTimeout)
            .header("Content-Type", "application/json")
            .header("User-Agent", "spark-cloudant")
            .auth(config.username, config.password)
      }

      clRequest.exec((code, headers, is) => {
        if (code == 200) {
          // val reader = new InputStreamReader(is, StandardCharsets.UTF_8)
          var i = 0
          scala.io.Source.fromInputStream(is, "utf-8").getLines().foreach(line => {
            if (line.length() > 0) {
              queue.add(line)
            } else if (i >= limit) {
              isThreadStopped = true
              return
            }
            i += 1
          })
        } else {
          val status = headers.getOrElse("Status", IndexedSeq.empty)
          val errorMsg = "Error retrieving _changes feed " + config.getDbname + ": " + status(0)
          new CloudantException(errorMsg)
        }
      })
      }

    def getStop: Boolean = isThreadStopped

    }

  class ChangesFeedConsumer[T](queue: BlockingQueue[String],
                                    postProcessor: (String) => T) extends Runnable {
    private val dataBuffer =
      scala.collection.mutable.ListBuffer.empty[T]
    override def run() {
      while (!queue.isEmpty) {
        val item = queue.take()
        // consume(item)
        val data = postProcessor(item)
        dataBuffer += data
        wait(2000)
      }
    }

    def getData: Seq[T] = dataBuffer
  }
