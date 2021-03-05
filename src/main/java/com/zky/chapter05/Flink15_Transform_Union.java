package com.zky.chapter05;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author：Dawn
 * @date：2020/11/27 10:08
 * @Desc： Union连接流, 要求流的数据类型要相同, 可以连接多条流
 */
public class Flink15_Transform_Union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //3条数据类型相同的流
        DataStreamSource<Integer> numDS1 = env.fromElements(1, 2, 3, 4, 5, 6);
        DataStreamSource<Integer> numDS2 = env.fromElements(11, 22, 33, 44, 55, 66);
        DataStreamSource<Integer> numDS3 = env.fromElements(111, 222, 333, 444, 555, 666);


        DataStream<Integer> unionDS = numDS1
                .union(numDS2)
                .union(numDS3);

        unionDS.print();

        env.execute();
    }
}
