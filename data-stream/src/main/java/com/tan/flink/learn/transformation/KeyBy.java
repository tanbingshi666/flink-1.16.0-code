package com.tan.flink.learn.transformation;

import lombok.Data;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * author name: tanbingshi
 * create time: 2022/11/16 15:27
 * describe content: flink-1.16.0-learn
 */
public class KeyBy {

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
                // 第一种
                .keyBy(WordCount::getWord)
                // 第二种
//                .keyBy(wordCount -> wordCount.word)
                // 第三种
//                .keyBy("count")
                // 第四种
//                .keyBy(0)
        ;

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
