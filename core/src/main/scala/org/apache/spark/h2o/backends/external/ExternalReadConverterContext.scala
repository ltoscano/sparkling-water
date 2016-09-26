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

package org.apache.spark.h2o.backends.external

import org.apache.spark.h2o.converters.ReadConverterContext
import org.apache.spark.h2o.utils.NodeDesc
import water.ExternalFrameReader
/**
  *
  * @param keyName key name of frame to query data from
  * @param chunkIdx chunk index
  * @param nodeDesc the h2o node which has data for chunk with the chunkIdx
  */
class ExternalReadConverterContext(override val keyName: String, override val chunkIdx: Int,
                                    val nodeDesc: NodeDesc, expectedTypes: Array[Byte], selectedColumnIndices: Array[Int])
  extends ExternalBackendUtils with ReadConverterContext {
  override type DataSource = ExternalFrameReader

  private val socketChannel = ConnectionToH2OHelper.getOrCreateConnection(nodeDesc)
  val externalFrameReader = new ExternalFrameReader(socketChannel, keyName, expectedTypes, chunkIdx, selectedColumnIndices)

  private val numOfRows: Int =  externalFrameReader.getNumOfRows()

  override def numRows: Int = numOfRows

  override def returnOption[T](read: DataSource => T)(columnNum: Int): Option[T] = {
    if (isNA(columnNum)) {
      None
    } else {
      Option(read(externalFrameReader))
    }
  }

  override def returnSimple[T](ifMissing: String => T, read: DataSource => T)(columnNum: Int): T = {
    if (isNA(columnNum)) ifMissing(s"Row $rowIdx column $columnNum") else read(externalFrameReader)
  }

  override def longAt(source: DataSource): Long = source.readLong()
  override def doubleAt(source: DataSource): Double = source.readDouble()
  override def string(source: DataSource) = source.readString()

  private def isNA(columnNum: Int) = externalFrameReader.readIsNA()

  override def hasNext: Boolean = {
    val isNext = super.hasNext
    if(!isNext){
      externalFrameReader.waitUntilAllReceived()
      // put back socket channel to pool of opened connections after we get the last element
      ConnectionToH2OHelper.putAvailableConnection(nodeDesc, socketChannel)
    }
    isNext
  }
}
