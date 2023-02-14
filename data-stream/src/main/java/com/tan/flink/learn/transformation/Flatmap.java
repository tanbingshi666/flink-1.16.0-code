package com.tan.flink.learn.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * author name: tanbingshi
 * create time: 2022/11/16 15:18
 * describe content: flink-1.16.0-learn
 */
public class Flatmap {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("hadoop", 10000);
        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String in, Collector<String> collector) throws Exception {
                // in ="hello,world,flink"
                for (String temp : in.split(",")) {
                    // out1 = hello
                    // out2 = world
                    // out3 = flink
                    collector.collect(temp);
                }

            }
        });

    }

}
