/*
 * Copyright 2015 Fabian Hueske / Vasia Kalavri
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.scala.examples.mypackage

import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext

import scala.util.Random

/**
  * Flink SourceFunction to generate SensorReadings with random temperature values.
  *
  * Each parallel instance of the source simulates 10 sensors which emit one sensor
  * reading every 100 ms.
  *
  * Note: This is a simple data-generating source function that does not checkpoint its state.
  * In case of a failure, the source does not replay any data.
  */
class RoomSource extends RichParallelSourceFunction[RoomReading] {

  // flag indicating whether source is still running.
  var running: Boolean = true

  /** run() continuously emits SensorReadings by emitting them through the SourceContext. */
  override def run(srcCtx: SourceContext[RoomReading]): Unit = {

    // initialize random number generator
    val rand = new Random()
    // look up index of this parallel task
    val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask

//    var curFTemp = (1 to 10).map {
////      i => ("room_" + (taskIdx * 10 + i), 65 + (rand.nextGaussian() * 20))
//        i => ("user_" + (taskIdx * 10 + i), if( i%2 == 0 ) 7 else 9 )
//    }

    var curFTemp = (1 to 3).map {
      //      i => ("room_" + (taskIdx * 10 + i), 65 + (rand.nextGaussian() * 20))
      i => ("user_" + (taskIdx * 10 + i), "init" )
    }

    var j = 0;
    while (running) {

      var state = "";
      val curTime = Calendar.getInstance.getTimeInMillis;

      {
          state = "join" + j

          //      curFTemp = curFTemp.map( t => (t._1, t._2) )
          curFTemp = curFTemp.map(t => (t._1, state))

          curFTemp.foreach(t => srcCtx.collect(RoomReading(t._1, curTime, t._2)))
      }

      {
          state = "leave" + j

          curFTemp = curFTemp.map( t => (t._1, state) )
          curFTemp.foreach(t => srcCtx.collect(RoomReading(t._1, curTime, t._2)))
      }

      Thread.sleep(1000*5)

      j = j + 1;
    }

  }

  /** Cancels this SourceFunction. */
  override def cancel(): Unit = {
    running = false
  }
}
