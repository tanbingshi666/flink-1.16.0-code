package com.tan.flink.learn.transformation.watermark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Calendar;
import java.util.Random;

/**
 * author name: tanbingshi
 * create time: 2022/11/17 13:32
 * describe content: flink-1.16.0-learn
 */
public class WatermarkSourceGenerator {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new CustomSourceEmitWatermark());

    }

    static class CustomSourceEmitWatermark implements SourceFunction<Event> {

        private boolean running = true;

        @Override
        public void run(SourceContext<Event> sourceContext) throws Exception {

            Random random = new Random();
            String[] userArr = {"Mary", "Bob", "Alice"};
            String[] urlArr = {"./home", "./cart", "./prod?id=1"};

            while (running) {

                String user = userArr[random.nextInt(userArr.length)];
                String url = urlArr[random.nextInt(urlArr.length)];
                long ts = Calendar.getInstance().getTimeInMillis();

                Event event = new Event(user, url, ts);

                // 使用 collectWithTimestamp 方法将数据发送出去，并指明数据中的时间戳的字段
                sourceContext.collectWithTimestamp(event, event.getTs());
                // 发送水位线
                sourceContext.emitWatermark(new Watermark(event.getTs() - 1L));

                Thread.sleep(1000L);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    static class Event {

        private String user;
        private String url;
        private Long ts;

        @Override
        public String toString() {
            return "Event{" +
                    "user='" + user + '\'' +
                    ", url='" + url + '\'' +
                    ", ts=" + ts +
                    '}';
        }
    }

}
