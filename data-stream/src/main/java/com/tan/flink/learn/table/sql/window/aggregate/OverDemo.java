package com.tan.flink.learn.table.sql.window.aggregate;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * author name: tanbingshi
 * create time: 2022/11/22 15:03
 * describe content: flink-1.16.0-learn
 * tan,aaa,1668670200000
 * tan,bbb,1668670201000
 * bing,ccc,1668670203000
 * shi,ddd,1668670204000
 * tan,eee,1668670205000
 * tan,fff,1668670202000
 * tan,ggg,1668670206000
 * tan,hhh,1668670211000
 * bing,ccc,1668670213000
 */
public class OverDemo {

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

        tableEnv.sqlQuery("SELECT `user`,\n" +
                        "    count(1) OVER(\n" +
                        "    PARTITION BY user\n" +
                        "    ORDER BY ts_ltz\n" +
                        "    RANGE BETWEEN INTERVAL '5' SECOND PRECEDING AND CURRENT ROW\n" +
                        "    ) as cnt\n" +
                        "FROM socket_info")
                .execute()
                .print();

    }

}
