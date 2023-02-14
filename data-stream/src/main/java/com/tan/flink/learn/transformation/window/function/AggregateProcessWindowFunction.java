package com.tan.flink.learn.transformation.window.function;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

/**
 * author name: tanbingshi
 * create time: 2022/11/17 16:26
 * describe content: flink-1.16.0-learn
 * <p>
 * * 2022-11-17 15:30:00
 * * tan,aaa,1668670200000
 * * tan,bbb,1668670201000
 * * tan,ccc,1668670203000
 * * tan,ddd,1668670204000
 * * tan,eee,1668670205000
 */
public class AggregateProcessWindowFunction {

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
                .aggregate(new CustomAggregateFunction(), new CustomProcessWindowFunction())
                .print();

        env.execute("Aggregate Process Window Function Job");
    }

    static class CustomAggregateFunction implements AggregateFunction<Event, Tuple2<Long, Long>, String> {

        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return new Tuple2<>(10L, 0L);
        }

        @Override
        public Tuple2<Long, Long> add(Event in, Tuple2<Long, Long> acc) {
            acc.f1 += 1L;
            return acc;
        }

        @Override
        public String getResult(Tuple2<Long, Long> res) {
            return res.f0 + "##" + res.f1;
        }

        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> acc1, Tuple2<Long, Long> acc2) {
            return new Tuple2<>(acc1.f0 + acc2.f0, acc2.f1 + acc2.f1);
        }
    }

    static class CustomProcessWindowFunction extends ProcessWindowFunction<String, String, String, TimeWindow> {

        @Override
        public void process(String key,
                            ProcessWindowFunction<String, String, String, TimeWindow>.Context context,
                            Iterable<String> values,
                            Collector<String> collector) throws Exception {

            ArrayList<String> res = Lists.newArrayList(values.iterator());
            System.out.println("size = " + res.size());

            for (String str : res) {
                collector.collect(str);
            }
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
