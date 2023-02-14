package com.tan.flink.learn.table.sql.cep;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * author name: tanbingshi
 * create time: 2022/11/23 9:30
 * describe content: flink-1.16.0-learn
 * ACME,12,1,2022-11-22 10:00:00
 * ACME,17,2,2022-11-22 10:00:01
 * ACME,19,1,2022-11-22 10:00:02
 * ACME,21,3,2022-11-22 10:00:03
 * ACME,25,2,2022-11-22 10:00:04
 * ACME,18,1,2022-11-22 10:00:05
 * ACME,15,1,2022-11-22 10:00:06
 * ACME,14,2,2022-11-22 10:00:07
 * ACME,24,2,2022-11-22 10:00:08
 * ACME,25,2,2022-11-22 10:00:09
 * ACME,19,1,2022-11-22 10:00:10
 * ACME,12,1,2022-11-22 10:00:11
 * ACME,11,1,2022-11-22 10:00:12
 * ACME,14,1,2022-11-22 10:00:13
 */
public class CEPDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE socket_info (\n" +
                "  `symbol`         STRING,\n" +
                "  `price`          BIGINT,\n" +
                "  `tax`            BIGINT,\n" +
                "  `rowtime`        TIMESTAMP(3),\n" +
                "   WATERMARK FOR `rowtime` AS `rowtime`\n" +
                ") WITH (\n" +
                "        'connector' = 'socket',\n" +
                "        'hostname' = 'hadoop',\n" +
                "        'port' = '10000',\n" +
                "        'format' = 'csv'\n" +
                ")");

        tableEnv.sqlQuery("select * from socket_info").printSchema();

        tableEnv.sqlQuery("SELECT *\n" +
                        "FROM socket_info\n" +
                        "    MATCH_RECOGNIZE (\n" +
                        "        -- 分区\n" +
                        "        PARTITION BY symbol\n" +
                        "        -- 排序 默认升序 ASC\n" +
                        "        ORDER BY rowtime\n" +
                        "        -- 输出结果\n" +
                        "        MEASURES\n" +
                        "            -- 输出结果1 -> CEP 匹配第一条数据的 rowtime 字段\n" +
                        "            START_ROW.rowtime AS start_tstamp,\n" +
                        "            -- 输出结果2 -> CEP 匹配最后一条数据的 rowtime 字段\n" +
                        "            LAST(PRICE_DOWN.rowtime) AS bottom_tstamp,\n" +
                        "            -- 输出结果3 -> 下一个 CEP 匹配的第一条数据的 rowtime 字段\n" +
                        "            LAST(PRICE_UP.rowtime) AS end_tstamp\n" +
                        "        -- 输出方式 定义每个匹配项应产生多少行\n" +
                        "        ONE ROW PER MATCH\n" +
                        "        -- 指定下一个匹配的开始位置 这也是控制单个事件可以属于多少个不同匹配项的方法\n" +
                        "        -- 这里跳到下一个 CEP 匹配的第一条数据\n" +
                        "        AFTER MATCH SKIP TO LAST PRICE_UP\n" +
                        "        -- 该模式具有开始事件 START_ROW 然后是一个或多个 PRICE_DOWN 事件 并以 PRICE_UP 事件结束\n" +
                        "        -- START_ROW 是默认定义的 PRICE_DOWN/PRICE_UP 定义在 DEFINE \n" +
                        "        PATTERN (START_ROW PRICE_DOWN+ PRICE_UP)\n" +
                        "        DEFINE\n" +
                        "            -- 定义 PRICE_DOWN\n" +
                        "            PRICE_DOWN AS\n" +
                        "                -- 初始状态 (第二条数据到来) LAST(PRICE_DOWN.price, 1) -> PRICE_DOWN 上一条数据为 NULL (不包括 START_ROW)\n" +
                        "                -- 第二条数据的 price 小于 第一条数据的 price \n" +
                        "                (LAST(PRICE_DOWN.price, 1) IS NULL AND PRICE_DOWN.price < START_ROW.price) OR\n" +
                        "                    -- 后续操作 继续判断 price 持续下降\n" +
                        "                    PRICE_DOWN.price < LAST(PRICE_DOWN.price, 1),\n" +
                        "            -- 定义 PRICE_UP\n" +
                        "            PRICE_UP AS\n" +
                        "                -- CEP 匹配终止条件\n" +
                        "                PRICE_UP.price > LAST(PRICE_DOWN.price, 1)\n" +
                        "    ) MR")
                .execute()
                .print();

    }

}
