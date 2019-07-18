package org.apache.flink.streaming.scala.examples.mypackage

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListStateDescriptor, ListState, ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import scala.collection.JavaConverters._

object RoomKeyedStateFunction {

  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.getCheckpointConfig.setCheckpointInterval(10 * 1000)

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.getConfig.setAutoWatermarkInterval(1000L)

    val sensorData: DataStream[RoomReading] = env
      .addSource(new RoomSource)
      .assignTimestampsAndWatermarks(new RoomTimeAssigner)

//    val keyedSensorData: KeyedStream[RoomReading, String] = sensorData.keyBy(_.id)
    val keyedSensorData: KeyedStream[RoomReading, String] = sensorData.keyBy(_.userId)

//    val alerts: DataStream[(String, Double, Double)] = keyedSensorData
//      .flatMap(new RoomTemperatureAlertFunction(1.7))

    val alerts: DataStream[(String, String, Set[String])] = keyedSensorData
      .flatMap(new RoomTemperatureAlertFunction(1.7))

    alerts.print()

    env.execute("Generate Temperature Alerts")
  }
}

/**
  * The function emits an alert if the temperature measurement of a sensor changed by more than
  * a configured threshold compared to the last reading.
  *
  * @param threshold The threshold to raise an alert.
  */
class RoomTemperatureAlertFunction(val threshold: Double)
//    extends RichFlatMapFunction[RoomReading, (String, Double, Double)] {
  extends RichFlatMapFunction[RoomReading, (String, String, Set[String])] {

//  private var lastTempState: ValueState[Double] = _
  private var lastActionState: ListState[String] = _

  override def open(parameters: Configuration): Unit = {

//    val lastTempDescriptor = new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
//    lastTempState = getRuntimeContext.getState[Double](lastTempDescriptor)

    val lastActionDescriptor = new ListStateDescriptor[String]("lastAction", classOf[String])
    lastActionState = getRuntimeContext.getListState[String](lastActionDescriptor)
  }

//  override def flatMap(reading: RoomReading, out: Collector[(String, Double, Double)]): Unit = {
//
//    val lastTemp = lastTempState.value()
//
////    val tempDiff = (reading.temperature - lastTemp).abs
//    val tempDiff = (reading.action - lastTemp).abs
//    if (tempDiff > threshold) {
////      out.collect((reading.id, reading.temperature, tempDiff))
//      out.collect((reading.userId, reading.action, tempDiff))
//    }
//
////    this.lastTempState.update(reading.temperature)
//    this.lastTempState.update(reading.action)
//  }

  override def flatMap(reading: RoomReading, out: Collector[(String, String, Set[String])]): Unit = {
    /////
//    var actions: List[String] = List();
    var actions: Set[String] = Set();
    var itor = lastActionState.get().iterator()
    while(itor.hasNext){
      val action = itor.next()

//      actions = action +: actions
      actions = actions + (action)
    }
//    actions = reading.action +: actions
    actions = actions + (reading.action)
    if( reading.action.equals("leave") && actions.contains("join")  ) {
      actions = actions-"join"
      actions = actions-"leave"
    }

    out.collect((reading.userId, reading.action, actions))

    lastActionState.update(actions.toList.asJava)
  }
}
