package com.tan.flink.learn.table.module;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * author name: tanbingshi
 * create time: 2022/11/23 15:50
 * describe content: flink-1.16.0-learn
 */
public class ModuleDemo {

    public static void main(String[] args) {

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(settings);

        String[] modules = tableEnv.listModules();
        for (String module : modules) {
            System.out.println(module);
        }

    }

}
