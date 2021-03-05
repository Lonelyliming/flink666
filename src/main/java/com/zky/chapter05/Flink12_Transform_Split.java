package com.zky.chapter05;

import com.zky.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author：Dawn
 * @date：2020/11/27 9:44
 * @Desc： split算子，根据不同的条件把数据切分成多个流，有点类似侧输出流。这个算子源码推荐使用侧输出流来代替
 * 给不同数据流增加标记以便于从流中取出。
 */
public class Flink12_Transform_Split {
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
        sensorDS.split(new OutputSelector<WaterSensor>() {
            @Override
            public Iterable<String> select(WaterSensor value) {
                if (value.getVc() < 50) {
                    return Arrays.asList("normal");
                } else if (value.getVc() < 80) {
                    return Arrays.asList("warn");
                } else {
                    return Arrays.asList("alarm");
                }
            }
        });


        env.execute();
    }
}
