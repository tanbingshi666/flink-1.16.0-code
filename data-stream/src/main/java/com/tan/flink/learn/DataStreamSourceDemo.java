package com.tan.flink.learn;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * author name: tanbingshi
 * create time: 2022/11/16 14:17
 * describe content: flink-1.16.0-learn
 */
public class DataStreamSourceDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.readTextFile("filepath");

        env.readFile(null,"");

    }

}
