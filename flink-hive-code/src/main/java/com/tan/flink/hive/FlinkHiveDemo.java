package com.tan.flink.hive;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class FlinkHiveDemo {

    public static void main(String[] args) {

        System.setProperty("HADOOP_USER_NAME", "tanbs");

        EnvironmentSettings settings = EnvironmentSettings.inStreamingMode();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        String name = "myhive";
        String defaultDatabase = "test";
        String hiveConfDir = "D:\\project\\bigdata\\flink-1.16.0-learn\\flink-hive-code\\src\\main\\resources";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        tableEnv.registerCatalog("myhive", hive);

        // set the HiveCatalog as the current catalog of the session
        tableEnv.useCatalog("myhive");

//        tableEnv.executeSql("desc mykafka").print();
//        tableEnv.executeSql("insert into mykafka values ('tan',22)");
//        tableEnv.executeSql("drop table mykafka");
//        tableEnv.executeSql("create table test(id int,name string)");
        tableEnv.executeSql("show tables").print();

        tableEnv.executeSql("insert into student values(1006,'tanbs')");
        tableEnv.executeSql("select * from student").print();

    }
}
