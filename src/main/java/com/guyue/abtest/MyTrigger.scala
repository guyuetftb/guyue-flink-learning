package com.guyue.abtest

import org.apache.flink.streaming.api.windowing.triggers.Trigger.TriggerContext
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * 自定义的窗口触发器
 */
class MyTrigger extends Trigger[(String, Long), TimeWindow] {
  /**
   * 每个元素被添加到窗口的时候调用
   *
   * @param element
   * @param timestamp
   * @param window
   * @param ctx
   * @return
   */
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    val start_time = window.getStart
    println(s"窗口的开始时间: $start_time")
    val end_time = window.getEnd
    println(s"窗口的结束时间: $end_time")
    val timestamp = window.maxTimestamp()
    println(s"窗口的最大时间戳: $timestamp")
    val current_watermark = ctx.getCurrentWatermark
    println(s"当前的watermark时间: $current_watermark")

    if (element._1.equals("10001")) {
      println("触发了window的操作")
      TriggerResult.FIRE
    } else {
      //此时继续等待
      TriggerResult.CONTINUE
    }
  }

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    TriggerResult.CONTINUE
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
  }

  //上下文中获取状态中的值，并从定时器中清除这个值
  private def clearTimerForState(ctx: TriggerContext): Unit = {
  }
}
