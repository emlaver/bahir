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

class CloudantAllDocsConfig(protocol: String, host: String, dbName: String,
                            indexName: String = null, viewName: String = null)
                           (username: String, password: String, partitions: Int,
                            maxInPartition: Int, minInPartition: Int, requestTimeout: Long,
                            bulkSize: Int, schemaSampleSize: Int,
                            createDBOnSave: Boolean, apiReceiver: String,
                            useQuery: Boolean, queryLimit: Int)
  extends CloudantConfig(protocol, host, dbName, indexName, viewName)(username, password,
  partitions, maxInPartition, minInPartition, requestTimeout, bulkSize, schemaSampleSize,
  createDBOnSave, apiReceiver, useQuery, queryLimit) {

  override val defaultIndex: String = apiReceiver

  def getOneUrl: String = {
    dbUrl + defaultIndex + "?limit=1&include_docs=true"
  }

  override def getUrl(limit: Int, excludeDDoc: Boolean = false): String = {
    if (viewName == null) {
      val baseUrl = {
        if (excludeDDoc) {
          dbUrl + "/_all_docs?startkey=%22_design0/%22&include_docs=true"
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

  def getSubSetUrl (url: String, skip: Int, limit: Int, queryUsed: Boolean): String = {
    val suffix = {
      if (url.indexOf(JsonStoreConfigManager.ALL_DOCS_INDEX) > 0) {
        "include_docs=true&limit=" + limit + "&skip=" + skip
      } else if (viewName != null) {
        "limit=" + limit + "&skip=" + skip
      } else if (queryUsed) {
        ""
      } else {
        "include_docs=true&limit=" + limit
      } // TODO Index query does not support subset query. Should disable Partitioned loading?
    }
    if (suffix.length == 0) {
      url
    } else if (url.indexOf('?') > 0) {
      url + "&" + suffix
    }
    else {
      url + "?" + suffix
    }
  }
}
