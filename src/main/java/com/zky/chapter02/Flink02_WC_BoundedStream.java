package com.zky.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author：Dawn
 * @date：2020/11/25 9:12
 * @Desc： 有界的流处理
 */
public class Flink02_WC_BoundedStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> fsDS = env.readTextFile("input/word.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordOneDS = fsDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedDS = wordOneDS.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = keyedDS.sum(1);

        sumDS.print();

        //启动
        env.execute();
    }
}
