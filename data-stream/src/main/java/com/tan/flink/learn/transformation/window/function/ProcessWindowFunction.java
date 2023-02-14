package com.tan.flink.learn.transformation.window.function;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * author name: tanbingshi
 * create time: 2022/11/17 16:10
 * describe content: flink-1.16.0-learn
 */
public class ProcessWindowFunction {

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
                .process(new CustomProcessWindowFunction())
                .print();

        env.execute("Process Window Function Job");
    }

    static class CustomProcessWindowFunction extends org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<Event, String, String, TimeWindow> {

        @Override
        public void process(
                String key,
                org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction<Event, String, String, TimeWindow>.Context context,
                Iterable<Event> values, Collector<String> collector) throws Exception {

            StringBuilder ret = new StringBuilder();
            for (Event value : values) {
                ret.append(value.getUrl());
            }

            collector.collect(ret.toString());
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
