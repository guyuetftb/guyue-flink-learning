package com.jerome.sideoutput

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * This is Description
  *
  * @author Jerome丶子木
  * @date 2020/01/13
  */
object SideOutputTestScala {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val separator = "\n"

    val input = env.socketTextStream("localhost", 9010)

    val outputTag = OutputTag[String]("side-output-tag")

    val processStream = input.process(new ProcessFunction[String, (String, Int)] {
      override def processElement(value: String, ctx: ProcessFunction[String, (String, Int)]#Context, out: Collector[(String, Int)]): Unit = {
        if (value.length > 5) {
          ctx.output(outputTag, value)
        } else {
          out.collect((value, 1))
        }
      }
    })
    val sideOutputStream = processStream.getSideOutput(outputTag).map("side output is : " + _)

    sideOutputStream.print()

    processStream.print()

    env.execute("side output test")

  }

}
