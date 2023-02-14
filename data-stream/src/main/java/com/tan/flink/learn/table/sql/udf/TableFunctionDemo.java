package com.tan.flink.learn.table.sql.udf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * author name: tanbingshi
 * create time: 2022/11/23 14:17
 * describe content: flink-1.16.0-learn
 */
public class TableFunctionDemo {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE socket_info (\n" +
                "                `user` STRING\n" +
                "            ) WITH (\n" +
                "                'connector' = 'socket',\n" +
                "                'hostname' = 'hadoop',\n" +
                "                'port' = '10000',\n" +
                "                'format' = 'csv'\n" +
                ")");

        tableEnv.createTemporarySystemFunction("temp_tbl_func", CustomTableFunction.class);

        // EXAMPLE 1
        /**
         tableEnv.sqlQuery("SELECT `user`,word,len\n" +
         "from socket_info,LATERAL TABLE (temp_tbl_func(user))")
         .execute()
         .print();
         */

        // EXAMPLE 2
        /**
         tableEnv.sqlQuery("SELECT `user`,word,len\n" +
         "from socket_info\n" +
         "left join\n" +
         "LATERAL TABLE (temp_tbl_func(user))\n" +
         "on TRUE")
         .execute()
         .print();
         */

        // EXAMPLE 3
        tableEnv.sqlQuery("SELECT `user`,t_word,t_len\n" +
                        "from socket_info\n" +
                        "left join\n" +
                        "LATERAL TABLE (temp_tbl_func(user)) AS T(t_word,t_len)\n" +
                        "on TRUE")
                .execute()
                .print();

    }

    @FunctionHint(output = @DataTypeHint("ROW<word STRING,len INT>"))
    public static class CustomTableFunction extends TableFunction<Row> {

        public void eval(String in) {
            String[] fields = in.split(" ");
            for (String field : fields) {
                collect(Row.of(field, field.length()));
            }
        }

    }

}
