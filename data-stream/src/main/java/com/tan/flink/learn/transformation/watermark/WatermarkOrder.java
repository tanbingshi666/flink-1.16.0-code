package com.tan.flink.learn.transformation.watermark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * author name: tanbingshi
 * create time: 2022/11/17 11:23
 * describe content: flink-1.16.0-learn
 */
public class WatermarkOrder {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
        );

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
