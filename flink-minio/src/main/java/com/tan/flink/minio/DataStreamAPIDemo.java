package com.tan.flink.minio;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.SequenceGenerator;

public class DataStreamAPIDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.addSource(new DataGeneratorSource<>(
                        new SequenceGenerator<String>(1, 1000000) {
                            final RandomDataGenerator random = new RandomDataGenerator();

                            @Override
                            public String next() {
                                try {
                                    Thread.sleep(1000L);
                                } catch (InterruptedException e) {
                                    e.printStackTrace();
                                }
                                return "" +
                                        valuesToEmit.peek().intValue() +
                                        valuesToEmit.poll().longValue() +
                                        random.nextInt(1, 100) +
                                        random.nextInt(0, 1);
                            }
                        }
                )).returns(Types.STRING)
                .print();

        env.execute();
    }

}
