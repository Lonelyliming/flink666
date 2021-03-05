package com.zky.chapter05;

import com.zky.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author：Dawn
 * @date：2020/11/26 15:43
 * @Desc：
 */
public class Flink06_Transform_Map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputDS = env.readTextFile("input/sensor-data.log");

        SingleOutputStreamOperator<WaterSensor> sensorDS = inputDS.map(new MyMapFunction());

        sensorDS.print();

        env.execute();
    }
    public static class MyMapFunction implements MapFunction<String,WaterSensor>{
        @Override
        public WaterSensor map(String value) throws Exception {
            String[] fields = value.split(",");
            return new WaterSensor(fields[0], Long.valueOf(fields[1]), Integer.valueOf(fields[2]));
        }
    }
}
