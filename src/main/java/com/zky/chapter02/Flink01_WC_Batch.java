package com.zky.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author：Dawn
 * @date：2020/11/25 8:59
 * @Desc： flink 的批处理
 */
public class Flink01_WC_Batch {
    public static void main(String[] args) throws Exception {
        //1：获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.读取有界文件流
        DataSource<String> fsDS = env.readTextFile("input/word.txt");

        //3.处理数据，转换成（word，1）
        FlatMapOperator<String, Tuple2<String, Integer>> wordOneTuple = fsDS.flatMap(new MyFlatMapFunction());

        //4.分组
        UnsortedGrouping<Tuple2<String, Integer>> wordOneGrouped = wordOneTuple.groupBy(0);

        //5.聚合
        AggregateOperator<Tuple2<String, Integer>> wordOneAgged = wordOneGrouped.sum(1);

        //6.输出
        wordOneAgged.print();

        //启动，批处理不需要
    }

    public static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String,Integer>>{
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.split(" ");

            for (String word : words) {
                Tuple2<String, Integer> tuple2 = new Tuple2<>(word, 1);
                collector.collect(tuple2);
            }
        }
    }
}
