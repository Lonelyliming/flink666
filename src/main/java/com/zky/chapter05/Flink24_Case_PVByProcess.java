package com.zky.chapter05;

import com.zky.bean.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author Dawn
 * @Date 2021/1/1 18:38
 * @Desc 统计电商pv指标,使用process底层算子来实现
 */
public class Flink24_Case_PVByProcess {
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

        // 5 求和 => 实现 计数 的功能，没有count这种聚合算子
        // 一般找不到现成的算子，那就调用底层的 process
        SingleOutputStreamOperator<Long> processedPvDS = pvAndTuple2DS
                .keyBy(r -> r.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Long>() {
                    //使用变量来统计条数
                    private Long pvCount = 0L;

                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Long> out) throws Exception {
                        //来一条加1
                        pvCount++;
                        out.collect(pvCount);
                    }
                });

        processedPvDS.print();
        env.execute();

    }
}
