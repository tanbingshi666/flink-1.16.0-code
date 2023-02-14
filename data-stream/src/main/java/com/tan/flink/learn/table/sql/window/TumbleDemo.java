package com.tan.flink.learn.table.sql.window;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * author name: tanbingshi
 * create time: 2022/11/22 10:48
 * describe content: flink-1.16.0-learn
 * 2022-11-17 15:30:00
 * tan,aaa,1668670200000
 * tan,bbb,1668670201000
 * tan,ccc,1668670203000
 * tan,ddd,1668670204000
 * tan,eee,1668670205000
 * tan,fff,1668670202000
 * tan,ggg,1668670206000
 * tan,hhh,1668670202100
 * tan,jjj,1668670207000
 * tan,iii,1668670202200
 */
public class TumbleDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE socket_info (\n" +
                "                `user` STRING,\n" +
                "                `url` STRING,\n" +
                "                `ts` BIGINT,\n" +
                "                `ts_ltz` AS TO_TIMESTAMP_LTZ(ts, 3),\n" +
                "                WATERMARK FOR ts_ltz AS ts_ltz - INTERVAL '1' SECOND\n" +
                "            ) WITH (\n" +
                "                'connector' = 'socket',\n" +
                "                'hostname' = 'hadoop',\n" +
                "                'port' = '10000',\n" +
                "                'format' = 'csv'\n" +
                ")");

        tableEnv.sqlQuery("select * from socket_info").printSchema();

        // EXAMPLE 1
        /**
         tableEnv.sqlQuery("SELECT `user`,`url`,`ts`,`window_start`,`window_end`,`window_time` FROM TABLE (\n" +
         "TUMBLE(\n" +
         "    DATA => TABLE socket_info,\n" +
         "    TIMECOL => DESCRIPTOR(ts_ltz),\n" +
         "    SIZE => INTERVAL '5' SECONDS\n" +
         "))").execute().print();
         */

        // EXAMPLE 2
        /**
         * tan,aaa,1668670200000
         * tan,bbb,1668670201000
         * tan,ccc,1668670203000
         * tan,ddd,1668670204000
         * tan,eee,1668670205000
         * tan,fff,1668670202000
         * tan,ggg,1668670206000
         * tan,hhh,1668670211000
         */
        tableEnv.sqlQuery("SELECT `window_start`,`window_end`,count(1) as cnt FROM TABLE (\n" +
                        "TUMBLE(\n" +
                        "    TABLE socket_info,\n" +
                        "    DESCRIPTOR(ts_ltz),\n" +
                        "    INTERVAL '5' SECONDS\n" +
                        ")) group by window_start,window_end")
                .execute()
                .print();

    }

}
