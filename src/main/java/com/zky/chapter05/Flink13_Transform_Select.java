package com.zky.chapter05;

import com.zky.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author：Dawn
 * @date：2020/11/27 9:44
 * @Desc： select 获取之前split中打上标签的数据
 */
public class Flink13_Transform_Select {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorDS = env.readTextFile("input/sensor-data.log")
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] words = value.split(",");
                        return new WaterSensor(words[0], Long.valueOf(words[1]), Integer.valueOf(words[2]));
                    }
                });

        // TODO Split: 水位低于 50 正常，水位 [50，80) 警告， 水位高于 80 告警
        // split并不是真正的把流分开,只是给每个数据打上一个标签
   /*     SplitStream<WaterSensor> splitDS = sensorDS.split(new OutputSelector<WaterSensor>() {
            @Override
            public Iterable<String> select(WaterSensor value) {
                if (value.getVc() < 50) {
                    return Arrays.asList("normal","common");
                } else if (value.getVc() < 80) {
                    return Arrays.asList("warn","common");
                } else {
                    return Arrays.asList("alarm");
                }
            }
        });*/

        SplitStream<WaterSensor> splitDS = sensorDS.split(new OutputSelector<WaterSensor>() {
            int a = 80;
            @Override
            public Iterable<String> select(WaterSensor value) {
                if (value.getVc() < 50) {
                    return Arrays.asList("normal");
                } else if (value.getVc() < a) {
                    return Arrays.asList("normal");
                } else {
                    return Arrays.asList("alarm");
                }
            }
        });

        //TODO select
        // 通过之前的标签名，获取对应的流
        // 一个流可以起多个名字，取出的时候，给定一个名字就行
        DataStream<WaterSensor> normal = splitDS.select("normal");
        normal.print();
//        splitDS.select("common").print("通用");

//        splitDS.select("warn").print("warn");
//        splitDS.select("alarm").print("alarm");


        env.execute();
    }
}
