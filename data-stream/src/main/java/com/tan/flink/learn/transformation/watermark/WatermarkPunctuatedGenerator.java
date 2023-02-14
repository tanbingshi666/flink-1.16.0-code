package com.tan.flink.learn.transformation.watermark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * author name: tanbingshi
 * create time: 2022/11/17 13:15
 * describe content: flink-1.16.0-learn
 */
public class WatermarkPunctuatedGenerator {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setAutoWatermarkInterval(200L);

        DataStreamSource<String> source = env.socketTextStream("hadoop", 10000);

        source.map(line -> {
            String[] fields = line.split(",");
            return new Event(
                    fields[0],
                    fields[1],
                    Long.parseLong(fields[2])
            );
        }).assignTimestampsAndWatermarks(new CustomWatermarkStrategy());

    }

    static class CustomWatermarkStrategy implements WatermarkStrategy<Event> {

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event event, long currentTs) {
                    return event.getTs();
                }
            };
        }

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPunctuatedGenerator();
        }
    }

    static class CustomPunctuatedGenerator implements WatermarkGenerator<Event> {

        @Override
        public void onEvent(Event event, long currentTs, WatermarkOutput watermarkOutput) {
            // 只有在遇到特定的流数据时，才发出水位线
            if (event.getUser().equals("tan")) {
                watermarkOutput.emitWatermark(new Watermark(event.getTs() - 1));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            // 不需要周期生成 watermark
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
