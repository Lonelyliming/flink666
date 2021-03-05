package com.zky.chapter06;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * TODO
 *
 * @author Dawn
 * @version 1.0
 * @date 2021年1月4日09:36:54
 */
public class Flink02_2_Window_IncreAgg {
    public static void main(String[] args) throws Exception {
        // 0 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 9999);

        KeyedStream<Tuple2<String, Integer>, String> dataKS = socketDS
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return Tuple2.of(value, 1);
                    }
                })
                .keyBy(r -> r.f0);

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowDS = dataKS.timeWindow(Time.seconds(5));

        //TODO 增量聚合
        //reduce 也是来一条处理一条，只是输入和输出的结果比较固定
        //aggregate 来一条处理一条，窗口关闭的时候，才会输出一次结果

   /*     windowDS.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                System.out.println(value1 + "<---->" + value2);
                //输出一个sum
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        }).print();*/

        windowDS.aggregate(new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {

            /**
             * 创建累加器 =》 初始化  => 这边做的是累加，那么给个初始值 0
             * @return
             */
            @Override
            public Integer createAccumulator() {
                System.out.println("init...");
                return 0;
            }

            /**
             * 如何进行累加
             * @param value
             * @param accumulator
             * @return
             */
            @Override
            public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
                System.out.println("add...");
                return accumulator + value.f1;
            }

            /**
             * 返回输出结果
             * @param accumulator
             * @return
             */
            @Override
            public Integer getResult(Integer accumulator) {
                System.out.println("result...");
                return accumulator;
            }

            /**
             * 合并累加器结果，只有会话窗口才会调用
             * @param a
             * @param b
             * @return
             */
            @Override
            public Integer merge(Integer a, Integer b) {
                return a + b;
            }
        }).print();


        env.execute();
    }
}
