package com.tan.flink.learn.table.sql.window.aggregate;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * author name: tanbingshi
 * create time: 2022/11/22 17:08
 * describe content: flink-1.16.0-learn
 * 2020-04-15 08:05:00,4.00,A,supplier1
 * 2020-04-15 08:06:00,4.00,C,supplier2
 * 2020-04-15 08:07:00,2.00,G,supplier1
 * 2020-04-15 08:08:00,2.00,B,supplier3
 * 2020-04-15 08:09:00,5.00,D,supplier4
 * 2020-04-15 08:11:00,2.00,B,supplier3
 * 2020-04-15 08:13:00,1.00,E,supplier1
 * 2020-04-15 08:15:00,3.00,H,supplier2
 * 2020-04-15 08:17:00,6.00,F,supplier5
 */
public class WindowTopNDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE socket_info (\n" +
                "  `bidtime`        TIMESTAMP(3),\n" +
                "  `price`          DECIMAL(10, 2),\n" +
                "  `item`           STRING,\n" +
                "  `supplier_id`    STRING,\n" +
                "  WATERMARK FOR `bidtime` AS `bidtime` - INTERVAL '1' SECOND\n" +
                ") WITH (\n" +
                "        'connector' = 'socket',\n" +
                "        'hostname' = 'hadoop',\n" +
                "        'port' = '10000',\n" +
                "        'format' = 'csv'\n" +
                ")");

        tableEnv.sqlQuery("select * from socket_info").printSchema();

        // EXAMPLE 1
        /**
         tableEnv.sqlQuery("SELECT *\n" +
         "  FROM (\n" +
         "    SELECT *, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY price DESC) as rownum\n" +
         "    FROM (\n" +
         "      SELECT window_start, window_end, supplier_id, SUM(price) as price, COUNT(1) as cnt\n" +
         "      FROM TABLE(\n" +
         "        TUMBLE(TABLE socket_info, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES))\n" +
         "      GROUP BY window_start, window_end, supplier_id\n" +
         "    )\n" +
         "  ) WHERE rownum <= 3")
         .execute()
         .print();
         */

        // EXAMPLE 2
        tableEnv.sqlQuery("SELECT *\n" +
                        "  FROM (\n" +
                        "    SELECT bidtime, price, item, supplier_id, window_start, window_end, ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY price DESC) as rownum\n" +
                        "    FROM TABLE(\n" +
                        "               TUMBLE(TABLE socket_info, DESCRIPTOR(bidtime), INTERVAL '10' MINUTES))\n" +
                        "  ) WHERE rownum <= 3")
                .execute()
                .print();

    }

}
