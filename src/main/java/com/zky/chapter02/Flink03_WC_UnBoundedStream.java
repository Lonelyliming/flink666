package com.zky.chapter02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author：Dawn
 * @date：2020/11/25 9:18
 * @Desc： 从netcat中获取无线流
 */
public class Flink03_WC_UnBoundedStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> socketDS = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordOneDS = socketDS.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (r, out) -> {
            String[] words = r.split(" ");

            for (String word : words) {
                out.collect(Tuple2.of(word, 1));
            }
        })
                .returns(new TypeHint<Tuple2<String, Integer>>() {
                });

        KeyedStream<Tuple2<String, Integer>, Tuple> keyedDS = wordOneDS.keyBy(0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> reslutDS = keyedDS.sum(1);

        reslutDS.print();

        env.execute();

    }
}
