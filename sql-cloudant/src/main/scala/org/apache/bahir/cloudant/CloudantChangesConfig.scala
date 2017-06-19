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

import org.apache.bahir.cloudant.common.JsonStoreConfigManager

class CloudantChangesConfig(protocol: String, host: String, dbName: String,
                            indexName: String = null, viewName: String = null)
                           (username: String, password: String, partitions: Int,
                            maxInPartition: Int, minInPartition: Int, requestTimeout: Long,
                            bulkSize: Int, schemaSampleSize: Int,
                            createDBOnSave: Boolean, apiReceiver: String, selector: String,
                            useQuery: Boolean, queryLimit: Int)
  extends CloudantConfig(protocol, host, dbName, indexName, viewName)(username, password,
    partitions, maxInPartition, minInPartition, requestTimeout, bulkSize, schemaSampleSize,
    createDBOnSave, apiReceiver, selector, useQuery, queryLimit) {

  override val defaultIndex: String = apiReceiver

  def getChangesUrl: String = {
    dbUrl + "/" + defaultIndex + "?include_docs=true&feed=normal"
  }

  def getContinuousChangesUrl: String = {
    var url = dbUrl + "/" + defaultIndex + "?include_docs=true&feed=continuous&heartbeat=3000"
    if (selector != null) {
      url = url + "&filter=_selector"
    }
    url
  }

  override def getTotalUrl(url: String): String = {
    if (url.contains('?')) {
      if (selector != null) {
        url + "&limit=1&filter=_selector"
      } else {
        url + "&limit=1"
      }
    } else {
      if (selector != null) {
        url + "?limit=1&filter=_selector"
      } else {
        url + "?limit=1"
      }
    }
  }

  override def getLastUrl(skip: Int): String = {
    if (skip == 0) {
      null
    } else {
      if (selector != null) {
        dbUrl + "/" + defaultIndex + "?limit=" + skip + "&filter=_selector"
      } else {
        dbUrl + "/" + defaultIndex + "?limit=" + skip
      }
    }
  }

  def getSubSetUrl (url: String, skip: Int, limit: Int, queryUsed: Boolean): String = {
    val suffix = {
      if (url.indexOf(JsonStoreConfigManager.CHANGES_INDEX) > 0) {
        if (selector != null) {
          "include_docs=true&limit=" + limit + "&since=" +
            skip.toString + "&filter=_selector"
        } else {
          "include_docs=true&limit=" + limit + "&since=" + skip.toString
        }
      } else {
        null
      }
    }
    super.getSubSetUrl(url, skip, limit, queryUsed, suffix)
  }

  override def getUrl(limit: Int, excludeDDoc: Boolean = false): String = {
    if (viewName == null) {
      val baseUrl = {
        if (excludeDDoc) {
          dbUrl + "/_all_docs?filter=_selector&include_docs=true"
        } else {
          dbUrl + "/_all_docs?include_docs=true"
        }
      }
      if (limit == JsonStoreConfigManager.ALLDOCS_OR_CHANGES_LIMIT) {
        baseUrl
      } else {
        baseUrl + "&limit=" + limit
      }
    } else {
      super.getUrl(limit)
    }
  }
}
