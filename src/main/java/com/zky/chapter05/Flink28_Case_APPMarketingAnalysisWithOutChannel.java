package com.zky.chapter05;

import com.zky.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @Author Dawn
 * @Date 2021/1/1 19:32
 * @Desc 不同行为 的统计
 */
public class Flink28_Case_APPMarketingAnalysisWithOutChannel {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<MarketingUserBehavior> appDS = env.addSource(new AppSource());

        //将每一条行为数据 装换成（渠道_行为， 1）
        SingleOutputStreamOperator<Tuple2<String, Integer>> channelAndBehaviorTupleDS = appDS.map(new MapFunction<MarketingUserBehavior, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(MarketingUserBehavior value) throws Exception {
                return Tuple2.of( value.getBehavior(), 1);
            }
        });

        //按照渠道、行为分组，将同一个渠道、行为的用户分到一组。
        KeyedStream<Tuple2<String, Integer>, String> keyDS = channelAndBehaviorTupleDS.keyBy(r -> r.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> resultDS = keyDS.sum(1);

        resultDS.print("app marketing analysis by channel and behavior");

        env.execute();
    }

//自定义一个数据源
public static class AppSource implements SourceFunction<MarketingUserBehavior>{

    private boolean flag = true;
    private List<String> behaviorList = Arrays.asList("DOWNLOAD", "INSTALL", "UPDATE", "UNINSTALL");
    private List<String> channelList = Arrays.asList("XIAOMI", "HUAWEI", "OPPO", "VIVO");

    @Override
    public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
        while (flag){
            Random random = new Random();
            ctx.collect(
                    new MarketingUserBehavior(
                            Long.valueOf(random.nextInt(10)),
                            behaviorList.get(random.nextInt(behaviorList.size())),
                            channelList.get(random.nextInt(channelList.size())),
                            System.currentTimeMillis()
                    )

            );

            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}

}