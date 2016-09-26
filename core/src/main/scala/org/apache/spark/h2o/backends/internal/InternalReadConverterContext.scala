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

package org.apache.spark.h2o.backends.internal

import org.apache.spark.h2o.converters.ReadConverterContext
import water.fvec.{Chunk, Frame, Vec}
import water.parser.BufferedString
import water.{DKV, Key}

import scala.language.postfixOps

class InternalReadConverterContext(override val keyName: String, override val chunkIdx: Int) extends ReadConverterContext{

  override type DataSource = Chunk

  /** Lazily fetched H2OFrame from K/V store */
  private lazy val fr: Frame = underlyingFrame

  /** Chunks for this partition */
  private lazy val chks: Array[Chunk] = water.fvec.FrameUtils.getChunks(fr, chunkIdx)

  /** Number of rows in this partition */
  lazy val numRows = chks(0)._len

  private def underlyingFrame = DKV.get(Key.make(keyName)).get.asInstanceOf[Frame]

  override def returnOption[T](read: DataSource => T)(columnNum: Int): Option[T] = {
    for {
      chunk <- Option(chks(columnNum)) if !chunk.isNA(rowIdx)
      data <- Option(read(chunk))
    } yield data
  }

  override def returnSimple[T](ifMissing: String => T, read: DataSource => T)(columnNum: Int): T = {
    val chunk = chks(columnNum)
      if (chunk.isNA(rowIdx)) ifMissing(s"Row $rowIdx column $columnNum") else read(chunk)
  }

  override def longAt(source: DataSource) = source.at8(rowIdx)
  override def doubleAt(source: DataSource) = source.atd(rowIdx)
  override def string(source: DataSource) = StringProviders(source.vec().get_type())(source)

  private def categoricalString(source: DataSource) = source.vec().domain()(longAt(source).toInt)
  private def uuidString(source: DataSource) = new java.util.UUID(source.at16h(rowIdx), source.at16l(rowIdx)).toString
  private def plainString(source: DataSource) = source.atStr(new BufferedString(), rowIdx).toString

  private val StringProviders = Map[Byte, (DataSource => String)](
    Vec.T_CAT -> categoricalString,
    Vec.T_UUID -> uuidString,
    Vec.T_STR -> plainString
  ) withDefault((t: Byte) => {
    assert(assertion = false, s"Should never be here, type is $t")
    (_: Chunk) => null
  }
    )

}
