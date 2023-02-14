package com.tan.flink.learn.table.sql.window.aggregate;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * author name: tanbingshi
 * create time: 2022/11/22 14:11
 * describe content: flink-1.16.0-learn
 */
public class RollupDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE socket_info (\n" +
                "                `user` STRING,\n" +
                "                `url` STRING,\n" +
                "                `ts` BIGINT,\n" +
                "                `ts_ltz` AS TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "                WATERMARK FOR ts_ltz AS ts_ltz\n" +
                "            ) WITH (\n" +
                "                'connector' = 'socket',\n" +
                "                'hostname' = 'hadoop',\n" +
                "                'port' = '10000',\n" +
                "                'format' = 'csv'\n" +
                ")");

        tableEnv.sqlQuery("select * from socket_info").printSchema();

        tableEnv.sqlQuery("SELECT `window_start`, `window_end`, `user`, count(1) as price\n" +
                        "  FROM TABLE(\n" +
                        "    TUMBLE(TABLE socket_info, DESCRIPTOR(ts_ltz), INTERVAL '5' SECONDS))\n" +
                        "  GROUP BY window_start, window_end, ROLLUP (user)")
                .execute()
                .print();

    }

}
