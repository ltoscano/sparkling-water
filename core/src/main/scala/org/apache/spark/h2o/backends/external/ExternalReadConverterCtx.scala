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


import org.apache.spark.h2o.converters.ReadConverterCtx
import org.apache.spark.h2o.utils.NodeDesc
import water.ExternalFrameReaderClient
/**
  *
  * @param keyName key name of frame to query data from
  * @param chunkIdx chunk index
  * @param nodeDesc the h2o node which has data for chunk with the chunkIdx
  */
class ExternalReadConverterCtx(override val keyName: String, override val chunkIdx: Int,
                               val nodeDesc: NodeDesc, expectedTypes: Array[Byte], selectedColumnIndices: Array[Int])
  extends ReadConverterCtx {
  override type DataSource = ExternalFrameReaderClient

  private val socketChannel = ConnectionToH2OHelper.getOrCreateConnection(nodeDesc)
  val externalFrameReader = new ExternalFrameReaderClient(socketChannel, keyName, chunkIdx, selectedColumnIndices, expectedTypes)

  // Ee can't do this field lazy right away
  // the reader api expects us to read number of rows as the first read after creating external frame reader and we need
  // to ensure we do so
  private val numOfRows: Int = externalFrameReader.getNumRows

  override def numRows: Int = numOfRows

  override def returnOption[T](read: DataSource => T)(columnNum: Int): Option[T] = {
    Option(read(externalFrameReader)).filter( _ => !externalFrameReader.isLastNA)
  }

  override def returnSimple[T](ifMissing: String => T, read: DataSource => T)(columnNum: Int): T = {
    val value = read(externalFrameReader)
    if (externalFrameReader.isLastNA) ifMissing(s"Row $rowIdx column $columnNum") else value
  }

  override def longAt(source: DataSource): Long = source.readLong()
  override def doubleAt(source: DataSource): Double = source.readDouble()
  override def string(source: DataSource) = source.readString()

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
