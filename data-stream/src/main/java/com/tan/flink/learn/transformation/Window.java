package com.tan.flink.learn.transformation;

import lombok.Data;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * author name: tanbingshi
 * create time: 2022/11/16 15:27
 * describe content: flink-1.16.0-learn
 */
public class Window {

    public static void main(String[] args) {

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
                .window(TumblingEventTimeWindows.of(Time.seconds(5L)))
                // 第一个泛型 -> 窗口元素
                // 第二个泛型 -> keyBy 类型
                // 第三个泛型 -> 触发窗口函数计算的输出结果
                // 第四个泛型 -> 窗口类型
                .apply(new WindowFunction<WordCount, String, String, TimeWindow>() {
                    @Override
                    public void apply(String key,
                                      TimeWindow timeWindow,
                                      Iterable<WordCount> iterable,
                                      Collector<String> collector) throws Exception {
                        // 窗口业务处理逻辑
                    }
                });

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
