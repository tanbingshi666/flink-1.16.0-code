package com.tan.flink.learn.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * author name: tanbingshi
 * create time: 2022/11/16 15:22
 * describe content: flink-1.16.0-learn
 */
public class Filter {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("hadoop", 10000);
        source.filter(new FilterFunction<String>() {
            private String prefix = "filter";

            @Override
            public boolean filter(String in) throws Exception {
                // 如果 in 字符串以 filter 前缀开头 则该字符串将会输出到下游 否则被过滤
                return in.startsWith(prefix);
            }
        });

    }

}
