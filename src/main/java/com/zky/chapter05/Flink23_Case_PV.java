package com.zky.chapter05;

import com.zky.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author Dawn
 * @Date 2021/1/1 18:38
 * @Desc 统计电商pv指标
 */
public class Flink23_Case_PV {
    public static void main(String[] args) throws Exception {
        //1 获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2 读取文件中的数据，并将每一行数据装换成用户行为对象
        SingleOutputStreamOperator<UserBehavior> UserBehaviorDS = env.readTextFile("input/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] fields = value.split(",");

                        return new UserBehavior(
                                Long.valueOf(fields[0]),
                                Long.valueOf(fields[1]),
                                Integer.valueOf(fields[2]),
                                fields[3],
                                Long.valueOf(fields[4]));
                    }
                });

        //3 过滤掉不是pv操作的对象
        SingleOutputStreamOperator<UserBehavior> pvDS = UserBehaviorDS.filter(r -> "pv".equals(r.getBehavior()));
        //4 将UserBehavior对象  -》 转成二元组（"pv",1）
        SingleOutputStreamOperator<Tuple2<String, Integer>> pvAndTuple2DS = pvDS.map(new MapFunction<UserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(UserBehavior value) throws Exception {
                return Tuple2.of("pv", 1);
            }
        });

        //5 按照第一个位置的元素 分组 => 聚合算子只能在分组之后调用，也就是 keyedStream才能调用 sum
        pvAndTuple2DS
                .keyBy(r -> r.f0)
                .sum(1)
                .print();

        env.execute();

    }
}
