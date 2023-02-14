package com.tan.flink.learn.transformation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * author name: tanbingshi
 * create time: 2022/11/16 16:42
 * describe content: flink-1.16.0-learn
 */
public class WindowJoin {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> source1 = env.socketTextStream("hadoop", 10000)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String in) throws Exception {
                        String[] fields = in.split(",");
                        return new Event(fields[0], Long.parseLong(fields[1]));
                    }
                });

        SingleOutputStreamOperator<Event> source2 = env.socketTextStream("hadoop", 10001).map(new MapFunction<String, Event>() {
            @Override
            public Event map(String in) throws Exception {
                String[] fields = in.split(",");
                return new Event(fields[0], Long.parseLong(fields[1]));
            }
        });

        source1.join(source2)
                .where(Event::getKey)
                .equalTo(Event::getKey)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .apply(new JoinFunction<Event, Event, String>() {
                    @Override
                    public String join(Event first, Event second) throws Exception {
                        System.out.println(first);
                        System.out.println(second);

                        return "success";
                    }
                }).print();


        env.execute("Window Join Job");
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class Event {
        private String key;
        private Long value;

        public String toString() {
            return "key = " + getKey() + "\t value = " + getValue();
        }
    }

}
