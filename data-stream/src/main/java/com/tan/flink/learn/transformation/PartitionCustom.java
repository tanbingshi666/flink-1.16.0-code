package com.tan.flink.learn.transformation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * author name: tanbingshi
 * create time: 2022/11/16 16:42
 * describe content: flink-1.16.0-learn
 */
public class PartitionCustom {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<Event> source = env.socketTextStream("hadoop", 10000)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String in) throws Exception {
                        String[] fields = in.split(",");
                        return new Event(fields[0], Long.parseLong(fields[1]));
                    }
                });

        source.partitionCustom(new CustomPartition(), new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) throws Exception {
                return event.getKey();
            }
        });

    }

    static class CustomPartition implements Partitioner<String> {

        @Override
        public int partition(String key, int i) {
            return key.hashCode() % i + i;
        }
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
