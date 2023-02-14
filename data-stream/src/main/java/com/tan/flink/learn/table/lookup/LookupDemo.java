package com.tan.flink.learn.table.lookup;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * author name: tanbingshi
 * create time: 2022/11/24 13:16
 * describe content: flink-1.16.0-learn
 * CREATE TABLE dim_user(
 * `id` int,
 * `name` VARCHAR(10),
 * `sex` int,
 * `age` int,
 * `address` VARCHAR(10),
 * primary key (id)
 * )
 * <p>
 * insert into dim_user values
 * (1001,'tan',0,22,'A'),
 * (1002,'bing',1,23,'B'),
 * (1003,'shi',2,24,'C'),
 * (1004,'test',3,25,'D');
 * <p>
 * insert into dim_user values
 * (1005,'eee',0,24,'E')
 * <p>
 * update table dim_user set name='update' where id =1002;
 * <p>
 * CREATE TABLE dim_user (
 * `id` INT,
 * `name` STRING,
 * `sex` INT,
 * `age` INT,
 * `address` STRING,
 * PRIMARY KEY (id) NOT ENFORCED
 * ) WITH (
 * 'connector' = 'jdbc',
 * 'url' = 'jdbc:mysql://hadoop:3306/flink',
 * 'table-name' = 'dim_user',
 * 'username' = 'root',
 * 'password' = '128505tan'
 * )
 * <p>
 * <p>
 * CREATE TABLE fact_user (
 * `id` INT,
 * `url` STRING,
 * `ts` BIGINT,
 * `protime` as PROCTIME()
 * ) WITH (
 * 'connector' = 'socket',
 * 'hostname' = 'hadoop',
 * 'port' = '10000',
 * 'format' = 'csv'
 * )
 * <p>
 * 1001,aaa,1668670200000
 * 1002,bbb,1668670201000
 * 1005,ccc,1668670203000
 * 1002,ddd,1668670204000
 * 1003,eee,1668670205000
 * <p>
 * SELECT
 * f.id as id,
 * f.url as url,
 * d.name as name,
 * d.sex as sex,
 * d.age as age,
 * d.address as address
 * from fact_user as f
 * join dim_user FOR SYSTEM_TIME AS OF f.protime as d
 * on f.id = d.id
 */
public class LookupDemo {

    public static void main(String[] args) {

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        tableEnv.executeSql("CREATE TABLE fact_user (\n" +
                "                `id` INT,\n" +
                "                `url` STRING,\n" +
                "                `ts` BIGINT,\n" +
                "                `protime` as PROCTIME()\n" +
                "            ) WITH (\n" +
                "                'connector' = 'socket',\n" +
                "                'hostname' = 'hadoop',\n" +
                "                'port' = '10000',\n" +
                "                'format' = 'csv'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE dim_user (\n" +
                "  `id` INT,\n" +
                "  `name` STRING,\n" +
                "  `sex` INT,\n" +
                "  `age` INT,\n" +
                "  `address` STRING,\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "   'connector' = 'jdbc',\n" +
                "   'url' = 'jdbc:mysql://hadoop:3306/flink',\n" +
                "   'table-name' = 'dim_user',\n" +
                "   'username' = 'root',\n" +
                "   'password' = '128505tan'\n" +
                ")");

        // query mysql for test
        /**
         tableEnv.sqlQuery("select * from dim_user")
         .execute()
         .print();
         */

        tableEnv.sqlQuery("SELECT\n" +
                        "    f.id as id,\n" +
                        "    f.url as url,\n" +
                        "    d.name as name,\n" +
                        "    d.sex as sex,\n" +
                        "    d.age as age,\n" +
                        "    d.address as address\n" +
                        "from fact_user as f\n" +
                        "join dim_user FOR SYSTEM_TIME AS OF f.protime as d\n" +
                        "on f.id = d.id")
                .execute()
                .print();

    }

}
