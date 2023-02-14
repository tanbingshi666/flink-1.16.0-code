package com.tan.flink.learn.transformation.process.function;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * author name: tanbingshi
 * create time: 2022/11/18 11:03
 * describe content: flink-1.16.0-learn
 * 2022-11-17 15:30:00
 * tan,aaa,1668670200000
 * tan,bbb,1668670201000
 * tan,ccc,1668670202000
 */
public class KeyedProcessFunctionDemo {

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
                .process(new CustomKeyedProcessFunction())
                .print();

        env.execute("Process Function Job");
    }

    static class CustomKeyedProcessFunction extends KeyedProcessFunction<String, Event, String> {

        @Override
        public void processElement(Event in,
                                   KeyedProcessFunction<String, Event, String>.Context context,
                                   Collector<String> collector) throws Exception {

            System.out.println("current system time is " + context.timerService().currentProcessingTime());
            System.out.println("current event time is " + context.timestamp());
            System.out.println("current watermark is " + context.timerService().currentWatermark());

            // 业务处理逻辑
            System.out.println(in);

            // 注册定时器
            context.timerService().registerEventTimeTimer(context.timestamp() + 1000L);

            // 输出结果
            collector.collect("success");
        }

        @Override
        public void onTimer(long timestamp,
                            KeyedProcessFunction<String, Event, String>.OnTimerContext ctx,
                            Collector<String> out) throws Exception {
            // 触发定时器
            System.out.println("onTimer is fire with event time is " + timestamp);
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
