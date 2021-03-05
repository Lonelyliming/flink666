package com.zky.chapter06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Dawn
 * @Date 2021/1/4 9:54
 * @Desc 滚动窗口
 */
public class Flink02_Window_CountWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);

        KeyedStream<Tuple2<String, Integer>, String> dateDS = socketDS
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                })
                .keyBy(r -> r.f0);

        // TODO CountWindow
        // 根据 本组 数据条数 => 因为是 keyby之后开的窗
        // 在滑动窗口中，一个数据能属于多少个窗口？ => 窗口长度 / 滑动步长
        // 在滑动窗口中，每经过一个滑动步长，就会触发一个窗口的计算
        dateDS
                .countWindow(5,3)
                .sum(1)
                .print();


        env.execute();
    }
}
