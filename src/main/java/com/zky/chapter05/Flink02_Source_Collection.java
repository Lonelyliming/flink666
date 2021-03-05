package com.zky.chapter05;

import com.zky.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author：Dawn
 * @date：2020/11/26 14:46
 * @Desc： 从集合中读取数据
 */
public class Flink02_Source_Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorDS = env.fromCollection(
                Arrays.asList(
                        new WaterSensor("sensor_1", 11111111L, 11),
                        new WaterSensor("sensor_2", 22222222L, 22),
                        new WaterSensor("sensor_3", 33333333L, 33)
                )
        );

        sensorDS.print();

        env.execute();
    }
}
