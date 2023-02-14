package com.tan.flink.learn.transformation.process.function;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * author name: tanbingshi
 * create time: 2022/11/18 13:50
 * describe content: flink-1.16.0-learn
 * 2022-11-17 15:30:00
 * tan,aaa,1668670200000
 * tan,bbb,1668670201000
 */
public class SideOutputDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.socketTextStream("hadoop", 10000);

        OutputTag<String> outputTag = new OutputTag<String>("side-output") {
        };

        SingleOutputStreamOperator<Event> result = source.map(line -> {
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
        ).process(new ProcessFunction<Event, Event>() {
            @Override
            public void processElement(Event in,
                                       ProcessFunction<Event, Event>.Context context,
                                       Collector<Event> collector) throws Exception {

                // 主输出流
                collector.collect(in);

                // 侧输出/旁路输出
                context.output(outputTag, "SideOutput-" + in.user);
            }
        });

        SideOutputDataStream<String> sideOutput = result.getSideOutput(outputTag);
        sideOutput.print("side output -> ");
        result.print();

        env.execute("Side Output Demo Job");
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
