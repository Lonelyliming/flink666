package com.zky.chapter05;

import com.zky.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author：Dawn
 * @date：2020/11/26 14:58
 * @Desc： 自定义数据源
 */
public class Flink05_Source_MySource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> sensorDS = env.addSource(new MySourceFunction());

        sensorDS.print();

        env.execute();
    }

    public static class MySourceFunction implements SourceFunction<WaterSensor> {
        //定义一个标记，来控制数据的生成
        private boolean flag = true;

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {
            Random random = new Random();

            while (flag) {
                ctx.collect(
                        new WaterSensor(
                                "sensor_" + random.nextInt(3),
                                System.currentTimeMillis(),
                                random.nextInt(10) + 40
                        )
                );

                Thread.sleep(700L);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }
}
