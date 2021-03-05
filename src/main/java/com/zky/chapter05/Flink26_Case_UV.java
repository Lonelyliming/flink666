package com.zky.chapter05;

import com.zky.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;
import java.util.Set;

/**
 * @Author Dawn
 * @Date 2021/1/1 18:38
 * @Desc 统计电商uv指标, 使用process底层算子来实现
 */
public class Flink26_Case_UV {
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

        SingleOutputStreamOperator<Tuple2<String, Long>> uvTuple2DS = pvDS.map(new MapFunction<UserBehavior, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(UserBehavior value) throws Exception {
                return Tuple2.of("uv", value.getUserId());
            }
        });

        // 5 uv指标，也就是在pv的基础上，按照用户进行分组，这里使用set集合来对用户进行去重。后续考虑使用布隆过滤器
        SingleOutputStreamOperator<Integer> uvResultDS = uvTuple2DS
                .keyBy(r -> r.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Long>, Integer>() {
                    // 定义一个Set，用来去重并存放 userId
                    Set<Long> userSet = new HashSet<>();

                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Integer> out) throws Exception {
                        // 来一条数据，就把 userId存到 Set中
                        userSet.add(value.f1);
                        // 通过采集器，往下游发送 uv值 => Set中元素的个数，就是 UV值
                        out.collect(userSet.size());
                    }
                });


        uvResultDS.print();
        env.execute();

    }
}
