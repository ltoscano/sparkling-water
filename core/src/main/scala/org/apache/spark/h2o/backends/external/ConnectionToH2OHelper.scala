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

import java.nio.channels.ByteChannel

import org.apache.spark.h2o.utils.NodeDesc
import water.ExternalFrameUtils

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Helper containing connections to H2O
  */
object ConnectionToH2OHelper {

  // since one executor can work on multiple tasks at the same time it can also happen that it needs
  // to communicate with the same node using 2 or more connections at the given time. For this we use
  // this helper which internally stores connections to one node and remember which ones are being used and which
  // ones are free. Programmer then can get connection using getAvailableConnection. This method creates a new connection
  // if all connections are currently used or reuse the existing free one. Programmer needs to put the connection back to the
  // pool of available connections using the method putAvailableConnection
  private class PerOneNodeConnection(val nodeDesc: NodeDesc) {

    private def getConnection(nodeDesc: NodeDesc): ByteChannel= {
      ExternalFrameUtils.getConnection(nodeDesc.hostname, nodeDesc.port)
    }
    // ordered list of connections where the available connections are at the start of the list and the used at the end.
    private val availableConnections = new java.util.concurrent.ConcurrentLinkedQueue[(ByteChannel, Long)]()
    def availableConnection: ByteChannel = {
      if(availableConnections.isEmpty){
        getConnection(nodeDesc)
      }else{
        val socketChannel = availableConnections.poll()._1
        if(!socketChannel.isOpen){
          // connection closed, open a new one to replace it
          getConnection(nodeDesc)
        }else{
          socketChannel
        }
      }
    }



    def putAvailableConnection(sock: ByteChannel): Unit = {
      availableConnections.add((sock, System.currentTimeMillis()))

      // after each put start this cleaning thread
      val waitTime = 1000 // 1 second
      new Thread(){
        override def run(): Unit = {
          Thread.sleep(waitTime)
          availableConnections.asScala.filter{
          case (socket, lastTimeUsed) =>  if (System.currentTimeMillis() - lastTimeUsed > waitTime) {
            socket.close()
            false
          }else{
            true
          }
        }
        }
      }.start()
    }
  }

  // this map is created in each executor so we don't have to specify executor Id
  private[this] val connectionMap = mutable.HashMap.empty[NodeDesc, PerOneNodeConnection]

  def getOrCreateConnection(nodeDesc: NodeDesc): ByteChannel = connectionMap.synchronized{
    if(!connectionMap.contains(nodeDesc)){
      connectionMap += nodeDesc -> new PerOneNodeConnection(nodeDesc)
    }
    connectionMap(nodeDesc).availableConnection
  }

  def putAvailableConnection(nodeDesc: NodeDesc, sock: ByteChannel): Unit = connectionMap.synchronized{
    if(!connectionMap.contains(nodeDesc)){
      connectionMap += nodeDesc -> new PerOneNodeConnection(nodeDesc)
    }
    connectionMap(nodeDesc).putAvailableConnection(sock)
  }

}
