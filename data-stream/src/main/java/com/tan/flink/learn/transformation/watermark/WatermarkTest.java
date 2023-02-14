package com.tan.flink.learn.transformation.watermark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * author name: tanbingshi
 * create time: 2022/11/17 16:58
 * describe content: flink-1.16.0-learn
 * 2022-11-17 15:30:00
 * tan,aaa,1668670200000
 * tan,bbb,1668670201000
 * tan,ddd,1668670206000
 * tan,ccc,1668670203000
 * tan,eee,1668670207000
 * tan,fff,1668670204000
 * tan,ggg,1668670212000
 */
public class WatermarkTest {

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
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2L))
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event in, long currentTs) {
                                        return in.getTs();
                                    }
                                })
                ).keyBy(Event::getUser)
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
                .process(new CustomProcessWindowFunction())
                .print();

        env.execute("Watermark Test Job");
    }

    static class CustomProcessWindowFunction extends ProcessWindowFunction<Event, String, String, TimeWindow> {

        @Override
        public void process(String key,
                            ProcessWindowFunction<Event, String, String, TimeWindow>.Context context,
                            Iterable<Event> values,
                            Collector<String> collector) throws Exception {

            long start = context.window().getStart();
            long end = context.window().getEnd();

            long currentWatermark = context.currentWatermark();
            long count = values.spliterator().getExactSizeIfKnown();

            collector.collect("窗口" + start + " ~ " + end + "中共有" + count + "个元素，窗口闭合计算时，水位线处于：" + currentWatermark);
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
