package com.zky.chapter05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author：Dawn
 * @date：2020/11/27 10:36
 * @Desc：
 */
public class Flink18_Transform_Process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //第一条sensor流
        KeyedStream<Tuple3<String, Long, Integer>, String> keyedDS = env.readTextFile("input/sensor-data.log")
                .map(new MapFunction<String, Tuple3<String, Long, Integer>>() {
                    @Override
                    public Tuple3<String, Long, Integer> map(String value) throws Exception {
                        String[] words = value.split(",");
                        return new Tuple3<>(words[0], Long.valueOf(words[1]), Integer.valueOf(words[2]));
                    }
                })
                .keyBy(r -> r.f0);

        keyedDS.process(new KeyedProcessFunction<String, Tuple3<String, Long, Integer>, String>() {
            /**
             * 处理数据的方法：来一条处理一条
             * @param value 一条数据
             * @param ctx   上下文
             * @param out   采集器
             * @throws Exception
             */
            @Override
            public void processElement(Tuple3<String, Long, Integer> value, Context ctx, Collector<String> out) throws Exception {
                out.collect("当前key=" + ctx.timerService().currentWatermark() + "当前时间=" + ctx.timestamp() + ",数据=" + value);
            }
        })
                .print("process 测试");

        env.execute();
    }


}
