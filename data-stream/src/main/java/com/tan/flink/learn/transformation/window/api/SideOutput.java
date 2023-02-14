package com.tan.flink.learn.transformation.window.api;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

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
 * tan,fff,1668670202000
 * tan,ggg,1668670206000
 * tan,ggg,1668670202020
 */
public class SideOutput {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop", 10000);

        final OutputTag<Event> lateOutputTag = new OutputTag<Event>("late-data") {
        };

        SingleOutputStreamOperator<String> result = source.map(line -> {
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
                .allowedLateness(Time.seconds(1L))
                .sideOutputLateData(lateOutputTag)
                .process(new CustomProcessWindowFunction());

        SideOutputDataStream<Event> sideOutput = result.getSideOutput(lateOutputTag);
        sideOutput.print("side output element");
        result.print();

        env.execute("Side Output Function Job");
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
