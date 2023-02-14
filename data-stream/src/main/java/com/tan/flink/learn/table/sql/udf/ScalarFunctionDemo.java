package com.tan.flink.learn.table.sql.udf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * author name: tanbingshi
 * create time: 2022/11/23 13:32
 * describe content: flink-1.16.0-learn
 */
public class ScalarFunctionDemo {

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

        tableEnv.createTemporaryFunction("temp_substring", CustomScalarFunction.class);

        tableEnv.sqlQuery("select user,temp_substring(url,1,3) as temp_url from socket_info")
                .execute()
                .print();

    }

    public static class CustomScalarFunction extends ScalarFunction {

        public String eval(String in, Integer begin, Integer end) {
            return in.substring(begin, end);
        }

    }

}


