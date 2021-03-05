package com.zky.chapter05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author：Dawn
 * @date：2020/11/26 16:09
 * @Desc：
 */
public class Flink09_Transform_Filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> inputDS = env.fromCollection(
                Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8)
        );

        inputDS
                .filter(new MyFilterFunction())
                .print("dawn");
        env.execute();
    }

    public static class MyFilterFunction implements FilterFunction<Integer> {
        @Override
        public boolean filter(Integer value) throws Exception {
            return value % 2 == 0;
        }
    }
}
