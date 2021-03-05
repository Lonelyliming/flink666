package com.zky.chapter05;

import com.zky.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author Dawn
 * @Date 2021/1/1 19:02
 * @Desc 统计电商pv指标, 使用FlatMap算子来实现过滤和结构装换, flatMap算子和map本质区别就是 input 可以!= output
 */
public class Flink25_Case_PVByFlatmap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> pvTupleDS = env
                .readTextFile("input/UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] fields = value.split(",");
                        if ("pv".equals(fields[3])) {
                            out.collect(Tuple2.of("pv", 1));
                        }
                    }
                });

        pvTupleDS
                .keyBy(0)
                .sum(1)
                .print();

        env.execute();
    }
}
