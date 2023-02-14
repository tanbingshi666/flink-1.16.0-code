package com.tan.flink.learn.transformation;

import lombok.Data;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * author name: tanbingshi
 * create time: 2022/11/16 15:27
 * describe content: flink-1.16.0-learn
 */
public class WindowReduce {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("hadoop", 10000);
        source.flatMap(new FlatMapFunction<String, WordCount>() {
                    @Override
                    public void flatMap(String in, Collector<WordCount> collector) throws Exception {
                        for (String temp : in.split(",")) {
                            collector.collect(new WordCount(temp, 1L));
                        }
                    }
                })
                .keyBy(WordCount::getWord)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5L)))
                .reduce(new ReduceFunction<WordCount>() {
                    @Override
                    public WordCount reduce(WordCount before, WordCount in) throws Exception {
                        // 业务处理逻辑
                        before.setCount(before.getCount() + in.getCount());
                        return before;
                    }
                }).print();

        env.execute("Non-Keyed Reduce");
    }

    @Data
    static class WordCount {

        public String word;
        public Long count;

        public WordCount(String word, Long count) {
            this.word = word;
            this.count = count;
        }
    }

}
