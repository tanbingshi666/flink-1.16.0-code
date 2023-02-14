package com.tan.flink.learn.table.sql.window.aggregate;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * author name: tanbingshi
 * create time: 2022/11/22 16:48
 * describe content: flink-1.16.0-learn
 * aaa,a1001,A,100
 * aaa,a1001,B,99
 * aaa,a1001,C,101
 * aaa,a1001,D,102
 */
public class TopNDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE socket_info (\n" +
                "  product_id   STRING,\n" +
                "  category     STRING,\n" +
                "  product_name STRING,\n" +
                "  sales        BIGINT\n" +
                ") WITH (\n" +
                "        'connector' = 'socket',\n" +
                "        'hostname' = 'hadoop',\n" +
                "        'port' = '10000',\n" +
                "        'format' = 'csv'\n" +
                ")");

        tableEnv.sqlQuery("select * from socket_info").printSchema();

        tableEnv.sqlQuery("SELECT product_id, category, product_name, sales\n" +
                        "FROM (\n" +
                        "  SELECT *,\n" +
                        "    ROW_NUMBER() OVER (PARTITION BY category ORDER BY sales DESC) AS row_num\n" +
                        "  FROM socket_info)\n" +
                        "WHERE row_num <= 3")
                .execute()
                .print();

    }

}
