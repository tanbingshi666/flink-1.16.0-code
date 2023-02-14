package com.tan.flink.learn.state;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * author name: tanbingshi
 * create time: 2022/11/19 10:09
 * describe content: flink-1.16.0-learn
 */
public class ValueStateDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("hadoop", 10000)
                .map(line -> {
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
                                    public long extractTimestamp(Event in, long currentTs) {
                                        return in.getTs();
                                    }
                                })
                ).keyBy(Event::getUser)
                .process(new CustomKeyedProcessFunction());
    }

    static class CustomKeyedProcessFunction extends KeyedProcessFunction<String, Event, String> {

        ValueState<Long> countState;
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {

            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
            timerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerTs", Long.class));
        }

        @Override
        public void processElement(Event in,
                                   KeyedProcessFunction<String, Event, String>.Context ctx,
                                   Collector<String> collector) throws Exception {

            Long count = countState.value();
            if (count == null) {
                countState.update(1L);
            } else {
                countState.update(count + 1);
            }

            if (timerTsState.value() == null) {
                ctx.timerService().registerEventTimeTimer(in.getTs() + 3 * 1000L);
                timerTsState.update(in.getTs() + 3 * 1000L);
            }

        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {

            out.collect("user = " + ctx.getCurrentKey() + "\t count = " + countState.value());

            // 清空状态
            timerTsState.clear();
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
