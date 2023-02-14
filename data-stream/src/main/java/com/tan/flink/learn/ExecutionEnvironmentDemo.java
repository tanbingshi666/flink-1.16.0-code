package com.tan.flink.learn;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * author name: tanbingshi
 * create time: 2022/11/16 13:57
 * describe content: flink-1.16.0-learn
 */
public class ExecutionEnvironmentDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 默认 STREAMING 模式
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // BATCH 模式
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        // 自动切换
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

//        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

//        StreamExecutionEnvironment.createRemoteEnvironment("host", 1, null);


    }

}
