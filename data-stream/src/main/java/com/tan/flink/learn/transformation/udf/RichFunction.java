package com.tan.flink.learn.transformation.udf;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * author name: tanbingshi
 * create time: 2022/11/17 9:17
 * describe content: flink-1.16.0-learn
 */
public class RichFunction {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("hadoop", 10000);
        source.map(new RichMapFunction<String, String>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                // 业务初始化操作
                super.open(parameters);
            }

            @Override
            public String map(String in) throws Exception {
                // 业务处理逻辑
                return null;
            }

            @Override
            public void close() throws Exception {
                // 关闭资源操作
                super.close();
            }
        });

    }

}
