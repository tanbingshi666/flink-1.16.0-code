package com.tan.flink.learn.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * author name: tanbingshi
 * create time: 2022/11/20 21:11
 * describe content: flink-1.16.0-learn
 */
public class TableEnvironmentDemo {

    public static void main(String[] args) {

        // 创建 TableEnvironment 两种方式

        // 第一种方式
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        // 第二种方式
        // 从源码看出 StreamTableEnvironment 继承 TableEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);
    }

}
