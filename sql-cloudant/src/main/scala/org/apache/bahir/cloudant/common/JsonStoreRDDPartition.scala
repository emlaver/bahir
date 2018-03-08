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
package org.apache.bahir.cloudant.common

import play.api.libs.json.JsValue

import org.apache.spark.Partition

import org.apache.bahir.cloudant.CloudantConfig

/**
   JsonStoreRDDPartition defines each partition as a subset of a query result:
   the limit rows returns and the skipped rows.
  */

class JsonStoreRDDPartition(val url: String, val skip: Int, val limit: Int,
                            val idx: Int, val config: CloudantConfig, val selector: JsValue,
                            val fields: JsValue, val queryUsed: Boolean)
  extends Partition with Serializable{
  val index: Int = idx
}
