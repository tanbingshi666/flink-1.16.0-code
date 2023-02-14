package com.tan.flink.learn.table.sql.ddl;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * author name: tanbingshi
 * create time: 2022/11/23 11:01
 * describe content: flink-1.16.0-learn
 */
public class CreateStatementDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE socket_info (\n" +
                "                `user` STRING,\n" +
                "                `url` STRING,\n" +
                "                `ts` BIGINT,\n" +
                "                `ts_ltz` AS TO_TIMESTAMP_LTZ(ts, 3)\n" +
                "            ) WITH (\n" +
                "                'connector' = 'socket',\n" +
                "                'hostname' = 'hadoop',\n" +
                "                'port' = '10000',\n" +
                "                'format' = 'csv'\n" +
                ")");

        tableEnv.executeSql("desc socket_info").print();

    }

}
