package com.tan.flink.learn.transformation.window.api;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * author name: tanbingshi
 * create time: 2022/11/18 9:11
 * describe content: flink-1.16.0-learn
 * <p>
 * 2022-11-17 15:30:00
 * tan,aaa,1668670200000
 * tan,bbb,1668670201000
 * tan,ccc,1668670203000
 * tan,ddd,1668670204000
 * tan,eee,1668670205000
 * tan,fff,1668670206000
 */
public class Trigger {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop", 10000);

        source.map(line -> {
                    String[] fields = line.split(",");
                    return new Event(
                            fields[0],
                            fields[1],
                            Long.parseLong(fields[2])
                    );
                }).assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event event, long currentTs) {
                                        return event.getTs();
                                    }
                                })
                ).keyBy(Event::getUser)
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .trigger(new CustomTrigger())
                .process(new CustomProcessWindowFunction())
                .print();

        env.execute("Trigger Function Job");
    }

    static class CustomTrigger extends org.apache.flink.streaming.api.windowing.triggers.Trigger<Event, TimeWindow> {

        private static final String FIRST_EVENT = "first-event";

        @Override
        public TriggerResult onElement(Event in, long currentTs, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

            // get or create
            ValueState<Boolean> isFirstEvent = triggerContext.getPartitionedState(new ValueStateDescriptor<Boolean>(FIRST_EVENT, Types.BOOLEAN));

            // 每个窗口每隔 1s 注册 eventTime 定时器 (每个窗口的第一条流数据执行)
            // 非第一条流数据直接跳过 但是底层会根据其 EventTime 判断是否触发之前注册过的定时器
            // 如果非第一条流数据的 EventTime 等于定时器触发的 EventTime 则调用 onEventTime()
            // onEventTime() 返回值是 FIRE 导致触发窗口计算函数 (也即调用 ProcessWindowFunction)
            // 依次类推 直到流数据全部到来 (暂不考虑延迟数据) 最后触发窗口计算函数 (该次触发是窗口关闭计算)
            if (isFirstEvent.value() == null) {
                for (long i = timeWindow.getStart(); i < timeWindow.getEnd(); i = i + 1000L) {
                    triggerContext.registerEventTimeTimer(i);
                }

                isFirstEvent.update(true);
            }

            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long currentTs, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long currentTs, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            System.out.println("onEventTime is Fire with eventTs = " + currentTs);
            return TriggerResult.FIRE;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            ValueState<Boolean> isFirstEvent = triggerContext.getPartitionedState(new ValueStateDescriptor<Boolean>(FIRST_EVENT, Types.BOOLEAN));
            isFirstEvent.clear();
        }
    }

    static class CustomProcessWindowFunction extends ProcessWindowFunction<Event, String, String, TimeWindow> {

        @Override
        public void process(String key,
                            ProcessWindowFunction<Event, String, String, TimeWindow>.Context context,
                            Iterable<Event> values,
                            Collector<String> collector) throws Exception {

            collector.collect(context.window().getStart() + " - " + context.window().getEnd() + " => size = " + values.spliterator().getExactSizeIfKnown());
        }

    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class Event {

        private String user;
        private String url;
        private Long ts;

        @Override
        public String toString() {
            return "Event{" +
                    "user='" + user + '\'' +
                    ", url='" + url + '\'' +
                    ", ts=" + ts +
                    '}';
        }
    }

}
