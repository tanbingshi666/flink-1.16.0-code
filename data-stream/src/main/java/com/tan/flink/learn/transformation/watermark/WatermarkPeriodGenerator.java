package com.tan.flink.learn.transformation.watermark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * author name: tanbingshi
 * create time: 2022/11/17 11:42
 * describe content: flink-1.16.0-learn
 */
public class WatermarkPeriodGenerator {

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
            // 提取时间戳
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event event, long currentTs) {
                    return event.getTs();
                }
            };
        }

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            // 自定义 watermark 生成
            return new CustomPeriodWatermarkGenerator();
        }

    }

    static class CustomPeriodWatermarkGenerator implements WatermarkGenerator<Event> {

        // 延迟时间
        private final Long delayTime = 5000L;

        // 观察到的最大时间戳
        private Long maxTs = Long.MIN_VALUE + delayTime + 1L;

        @Override
        public void onEvent(Event event, long currentTs, WatermarkOutput watermarkOutput) {
            // 每来一条 Event 进行判断
            maxTs = Math.max(event.getTs(), maxTs);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            // 默认每隔 200 ms 触发
            // -1 -> 左闭右开区间
            watermarkOutput.emitWatermark(new Watermark(maxTs - delayTime - 1L));
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
