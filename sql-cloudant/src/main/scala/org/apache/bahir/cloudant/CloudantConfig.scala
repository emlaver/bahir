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

import java.net.{URL, URLEncoder}
import java.nio.file.Paths

import scala.collection.JavaConversions._ // scalastyle:ignore

import com.cloudant.client.api.{ClientBuilder, CloudantClient, Database}
import com.cloudant.client.api.model.SearchResult
import com.cloudant.client.api.views._
import com.cloudant.http.Http
import com.google.gson.JsonObject
import play.api.libs.json.{JsArray, JsObject, Json, JsValue}

import org.apache.bahir.cloudant.common._

/*
* Only allow one field pushdown now
* as the filter today does not tell how to link the filters out And v.s. Or
*/

class CloudantConfig(val protocol: String, val host: String,
                     val dbName: String, val indexPath: String, val viewPath: String)
                    (implicit val username: String, val password: String,
                     val partitions: Int, val maxInPartition: Int, val minInPartition: Int,
                     val requestTimeout: Long, val bulkSize: Int, val schemaSampleSize: Int,
                     val createDBOnSave: Boolean, val endpoint: String,
                     val useQuery: Boolean = false, val queryLimit: Int)
  extends Serializable {

  @transient private lazy val client: CloudantClient = ClientBuilder
    .url(new URL(protocol + "://" + host))
    .username(username)
    .password(password)
    .build
  @transient private lazy val database: Database = client.database(dbName, false)
  lazy val dbUrl: String = {protocol + "://" + host + "/" + dbName}
  lazy val designDoc: String =
    Paths.get("_design", viewPath.substring(viewPath.indexOf("_design/") + 1)).toString
  lazy val searchName: String = indexPath.substring(indexPath.indexOf("_search/") + 1)
  lazy val viewName: String = viewPath.substring(viewPath.indexOf("_view/") + 1)

  val pkField = "_id"
  val defaultIndex: String = endpoint
  val default_filter: String = "*:*"

  def buildAllDocsRequest(limit: Int): AllDocsRequestBuilder = {
    var allDocsReq = database.getAllDocsRequestBuilder.includeDocs(true)
    if (limit != JsonStoreConfigManager.ALLDOCS_OR_CHANGES_LIMIT) {
      allDocsReq = allDocsReq.limit(limit)
    }
    allDocsReq
  }

  def buildViewRequest(limit: Int): UnpaginatedRequestBuilder[String, String] = {
    val viewReq = database.getViewRequestBuilder(designDoc, viewName)
      .newRequest(Key.Type.STRING, classOf[String])
      .includeDocs(true)
    if (limit != JsonStoreConfigManager.ALLDOCS_OR_CHANGES_LIMIT) {
      viewReq.limit(limit)
    }
    viewReq
  }

  def buildSearchRequest(limit: Int): SearchResult[String] = {
    val searchReq = database.search(searchName)
    if (limit != JsonStoreConfigManager.ALLDOCS_OR_CHANGES_LIMIT) {
      searchReq.limit(limit)
    }
    searchReq.querySearchResult(default_filter, classOf[String])
  }

  def getClient: CloudantClient = {
    client
  }

  def getDatabase: Database = {
    database
  }

  def getSchemaSampleSize: Int = {
    schemaSampleSize
  }

  def getCreateDBonSave: Boolean = {
    createDBOnSave
  }

  def getLastNum(result: JsValue): JsValue = (result \ "last_seq").get

  /* Url containing limit for docs in a Cloudant database.
  * If a view is not defined, use the _all_docs endpoint.
  * @return url with one doc limit for retrieving total doc count
  */
  def getUrl(limit: Int, excludeDDoc: Boolean = false): Seq[JsValue] = {
    // TODO exclude ddocs during load process
    if (viewPath == null) {
      val allDocsRows = buildAllDocsRequest(limit).build().getResponse.getDocsAs(classOf[JsValue])
      if(excludeDDoc) {
        allDocsRows.filter(r => FilterDDocs.filter(r))
      } else {
        allDocsRows
      }
    } else {
      val viewRows = buildViewRequest(limit).build().getResponse.getDocsAs(classOf[JsValue])
      if(excludeDDoc) {
        viewRows.filter(r => FilterDDocs.filter(r))
      } else {
        viewRows
      }
    }
  }

  /* Url containing limit to count total docs in a Cloudant database.
  *
  * @return url with one doc limit for retrieving total doc count
  */
  def getTotal(url: String = JsonStoreConfigManager.ALL_DOCS_INDEX): Int = {
    if (viewPath != null) {
      // "limit=" + limit + "&skip=" + skip
      val viewResp = buildViewRequest(1).build().getResponse.getDocsAs(classOf[String])
      getTotalRows(Json.parse(viewResp.get(0).toString))
    } else if (indexPath != null) {
      val searchResp = buildSearchRequest(1).getRows
      getTotalRows(Json.parse(searchResp.get(0).toString))
    } else {
      val response = client.executeRequest(Http.GET(
        new URL(database.getDBUri + "/_all_docs?limit=1")))
      getTotalRows(Json.parse(response.responseAsString))
    }
  }

  def getAllDocs: Seq[JsonObject] = {
    buildAllDocsRequest(-1).build().getResponse.getDocsAs(classOf[JsonObject]).toList
  }

  def getAllDocsResponse: String = {
    val response = client.executeRequest(Http.GET(
      new URL(database.getDBUri + "/_all_docs?include_docs=true")))
    response.responseAsString
  }

  def getDbname: String = {
    dbName
  }

  def queryEnabled: Boolean = {
    useQuery && indexPath == null && viewName == null
  }

  def allowPartition(queryUsed: Boolean): Boolean = {indexPath == null && !queryUsed}

  def getRangeUrl(field: String = null, start: Any = null,
                  startInclusive: Boolean = false, end: Any = null,
                  endInclusive: Boolean = false,
                  includeDoc: Boolean = true,
                  allowQuery: Boolean = false): (String, Boolean, Boolean) = {
    val (url: String, pusheddown: Boolean, queryUsed: Boolean) =
      calculate(field, start, startInclusive, end, endInclusive, allowQuery)
    if (includeDoc && !queryUsed ) {
      if (url.indexOf('?') > 0) {
        (url + "&include_docs=true", pusheddown, queryUsed)
      } else {
        (url + "?include_docs=true", pusheddown, queryUsed)
      }
    } else {
      (url, pusheddown, queryUsed)
    }
  }

  /*
  * Url for paging using skip and limit options when loading docs with partitions.
  */
  def getSubSetUrl[T](url: String, skip: Int, limit: Int, queryUsed: Boolean):
  Seq[JsonObject] = {
    if (url.indexOf(JsonStoreConfigManager.ALL_DOCS_INDEX) > 0) {
      // "include_docs=true&limit=" + limit + "&skip=" + skip
      buildAllDocsRequest(limit).skip(skip).build()
        .getResponse.getDocsAs(classOf[JsonObject]).toList
      // docs.removeIf(d => d.asInstanceOf[Map].get("_id").toString.startsWith("_design"))
    } else if (viewName != null) {
      // "limit=" + limit + "&skip=" + skip
      buildViewRequest(limit).skip(skip).build().getResponse
        .getDocs.toList
      // TODO
      null
    } else if (queryUsed) {
      // val find = database.findAny(classOf[String], url)
      null
    } else {
      // "include_docs=true&limit=" + limit
      var searchList = List[AllDocsDocument]()
      val rows = buildSearchRequest(limit).getRows
      for(row: SearchResult[String]#SearchResultRow <- rows) {
        // val test = Json.fromJson[AllDocsDocument](row.getDoc.toString)
        // searchList += Json.fromJson[AllDocsDocument](row.getDoc.toString)
        rows.remove(row)
      }
      // TODO
      null
    }
  }

  private def calculate(field: String, start: Any,
                        startInclusive: Boolean, end: Any, endInclusive: Boolean,
                        allowQuery: Boolean): (String, Boolean, Boolean) = {
    if (field != null && field.equals(pkField)) {
      var condition = ""
      if (start != null && end != null && start.equals(end)) {
        condition += "?key=%22" + URLEncoder.encode(start.toString, "UTF-8") + "%22"
      } else {
        if (start != null) {
          condition += "?startkey=%22" + URLEncoder.encode(
            start.toString, "UTF-8") + "%22"
        }
        if (end != null) {
          if (start != null) {
            condition += "&"
          } else {
            condition += "?"
          }
          condition += "endkey=%22" + URLEncoder.encode(end.toString, "UTF-8") + "%22"
        }
      }
      (dbUrl + "/" + defaultIndex + condition, true, false)
    } else if (indexPath != null) {
      //  push down to indexName
      val condition = calculateCondition(field, start, startInclusive,
        end, endInclusive)
      (dbUrl + "/" + indexPath + "?q=" + condition, true, false)
    } else if (viewPath != null) {
      (dbUrl + "/" + viewPath, false, false)
    } else if (allowQuery && useQuery) {
      (s"$dbUrl/_find", false, true)
    } else {
      (s"$dbUrl/$defaultIndex", false, false)
    }

  }

  def calculateCondition(field: String, min: Any, minInclusive: Boolean = false,
                         max: Any, maxInclusive: Boolean = false) : String = {
    if (field != null && (min != null || max!= null)) {
      var condition = field + ":"
      if (min!=null && max!=null && min.equals(max)) {
        condition += min
      } else {
        if (minInclusive) {
          condition+="["
        } else {
          condition +="{"
        }
        if (min!=null) {
          condition += min
        } else {
          condition+="*"
        }
        condition+=" TO "
        if (max !=null) {
          condition += max
        } else {
          condition += "*"
        }
        if (maxInclusive) {
          condition+="]"
        } else {
          condition +="}"
        }
      }
      URLEncoder.encode(condition, "UTF-8")
    } else {
      default_filter
    }
  }

  def getTotalRows(result: JsValue): Int = {
    val resultKeys = result.as[JsObject].keys
    if(resultKeys.contains("total_rows")) {
      (result \ "total_rows").as[Int]
    } else if (resultKeys.contains("pending")) {
      (result \ "pending").as[Int] + 1
    } else {
      1
    }
  }

  def getRows(result: JsValue, queryUsed: Boolean): Seq[JsValue] = {
    if ( queryUsed ) {
      (result \ "docs").as[JsArray].value.map(row => row)
    } else {
      val containsResultsKey: Boolean = result.as[JsObject].keys.contains("results")
      if (containsResultsKey) {
        (result \ "results").as[JsArray].value.map(row => (row \ "doc").get)
      } else if (viewName == null) {
        (result \ "rows").as[JsArray].value.map(row => (row \ "doc").get)
      } else {
        (result \ "rows").as[JsArray].value.map(row => row)
      }
    }
  }

  def getBulkPostUrl: String = {
    dbUrl + "/_bulk_docs"
  }

  def getBulkRows(rows: List[String]): String = {
    val docs = rows.map { x => Json.parse(x) }
    Json.stringify(Json.obj("docs" -> Json.toJson(docs.toSeq)))
  }

  def getConflictErrStr: String = {
    """"error":"conflict""""
  }

  def getForbiddenErrStr: String = {
    """"error":"forbidden""""
  }
}
