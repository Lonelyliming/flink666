package com.zky.chapter05;

import com.zky.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @author：Dawn
 * @date：2020/11/27 10:02
 * @Desc： 使用connect连接两条流，两条流 数据类型 可以不一样，只能两条流进行连接，处理数据的时候，也是分开处理
 */
public class Flink14_Transform_Connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //第一条sensor流
        SingleOutputStreamOperator<WaterSensor> sensorDS = env.readTextFile("input/sensor-data.log")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] words = value.split(",");
                        return new WaterSensor(words[0], Long.valueOf(words[1]), Integer.valueOf(words[2]));
                    }
                });

        //第二条 数字 流
        DataStreamSource<Integer> numDS = env.fromElements(1, 2, 3, 4, 5, 6);


        //2条流进行合并
        ConnectedStreams<WaterSensor, Integer> connectDS = sensorDS.connect(numDS);

        connectDS.map(new CoMapFunction<WaterSensor, Integer, Object>() {
            @Override
            public Object map1(WaterSensor value) throws Exception {
                return value.toString();
            }

            @Override
            public Object map2(Integer value) throws Exception {
                return value + 10;
            }
        }).print();

        env.execute();
    }
}
