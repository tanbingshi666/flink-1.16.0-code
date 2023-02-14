package com.tan.flink.learn.transformation;

import lombok.Data;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * author name: tanbingshi
 * create time: 2022/11/16 15:27
 * describe content: flink-1.16.0-learn
 */
public class Reduce {

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
                .reduce(new ReduceFunction<WordCount>() {
                    /**
                     *
                     * @param before   上个累计 WordCount
                     * @param in       输入数据元素
                     * @return 状态写入
                     * 类似 WordCount 程序
                     */
                    @Override
                    public WordCount reduce(WordCount before, WordCount in) throws Exception {
                        before.setCount(before.getCount() + in.getCount());
                        return before;
                    }
                }).print();

        env.execute("Reduce Job");

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
