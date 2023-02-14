package com.tan.flink.learn.transformation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * author name: tanbingshi
 * create time: 2022/11/16 16:42
 * describe content: flink-1.16.0-learn
 */
public class IntervalJoin {

//    public static void main(String[] args) throws Exception {
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        SingleOutputStreamOperator<Event> source1 = env.socketTextStream("hadoop", 10000)
//                .map(new MapFunction<String, Event>() {
//                    @Override
//                    public Event map(String in) throws Exception {
//                        String[] fields = in.split(",");
//                        return new Event(fields[0], Long.parseLong(fields[1]));
//                    }
//                });
//
//        SingleOutputStreamOperator<Event> source2 = env.socketTextStream("hadoop", 10001).map(new MapFunction<String, Event>() {
//            @Override
//            public Event map(String in) throws Exception {
//                String[] fields = in.split(",");
//                return new Event(fields[0], Long.parseLong(fields[1]));
//            }
//        });
//
//        // key1 == key2 && leftTs - 5 < rightTs < leftTs + 5
//        source1.keyBy(Event::getKey)
//                .intervalJoin(
//                        source2.keyBy(Event::getKey)
//                ).inProcessingTime()
//                .between(Time.seconds(-5), Time.seconds(5))
//                .lowerBoundExclusive()
//                .upperBoundExclusive()
//                .process(new ProcessJoinFunction<Event, Event, String>() {
//                    @Override
//                    public void processElement(
//                            Event first,
//                            Event second,
//                            ProcessJoinFunction<Event, Event, String>.Context context,
//                            Collector<String> collector) throws Exception {
//
//                        // 业务处理逻辑
//                        System.out.println(first);
//                        System.out.println(second);
//
//                        collector.collect("success");
//                    }
//                }).print();
//
//        env.execute("Window Join Job");
//    }
//
//    @Data
//    @NoArgsConstructor
//    @AllArgsConstructor
//    static class Event {
//        private String key;
//        private Long value;
//
//        public String toString() {
//            return "key = " + getKey() + "\t value = " + getValue();
//        }
//    }

}
