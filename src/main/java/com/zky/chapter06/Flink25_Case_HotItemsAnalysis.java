package com.zky.chapter06;

import com.zky.bean.HotItemCountWithWindowEnd;
import com.zky.bean.UserBehavior;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @Author Dawn
 * @Date 2021/1/5 15:44
 * @Desc 每隔5分钟输出最近一小时内点击量最多的前N个商品
 */
public class Flink25_Case_HotItemsAnalysis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<UserBehavior> userBehaviorDS = env
                .readTextFile("input/UserBehavior.csv")
                .map(new MapFunction<String, UserBehavior>() {
                    @Override
                    public UserBehavior map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new UserBehavior(
                                Long.valueOf(fields[0]),
                                Long.valueOf(fields[1]),
                                Integer.valueOf(fields[2]),
                                fields[3],
                                Long.valueOf(fields[4])
                        );
                    }
                })
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<UserBehavior>() {
                    @Override
                    public long extractAscendingTimestamp(UserBehavior element) {
                        return element.getTimestamp() * 1000L;
                    }
                });


        SingleOutputStreamOperator<UserBehavior> filterDS = userBehaviorDS.filter(r -> "pv".equals(r.getBehavior()));

        //按照商品进行分组
        KeyedStream<UserBehavior, Long> userBehaviorKS = filterDS.keyBy(r -> r.getItemId());

        WindowedStream<UserBehavior, Long, TimeWindow> userBehaviorWS = userBehaviorKS.timeWindow(Time.hours(1), Time.minutes(5));

        //统计每个商品的点击次数
        //如果直接使用processWindowFunction的话容易OOM，这里使用窗口的增量聚合来计算结果，用全窗口函数来获取窗口信息
        SingleOutputStreamOperator<HotItemCountWithWindowEnd> aggDS = userBehaviorWS.aggregate(new AggCount(), new CountResultWindowEnd());

        //按照窗口的结束时间进行排序
        KeyedStream<HotItemCountWithWindowEnd, Long> aggKS = aggDS.keyBy(r -> r.getWindowEnd());

        SingleOutputStreamOperator<String> topN = aggKS.process(new TopNItems(3));

        topN.print();

        env.execute();
    }

    public static class TopNItems extends KeyedProcessFunction<Long, HotItemCountWithWindowEnd, String> {
        private Integer topN;

        public TopNItems(int topN) {
            this.topN = topN;
        }

        //将来的一条一条数据保存起来，直到这个窗口结束
        private ListState<HotItemCountWithWindowEnd> dateList;
        private ValueState<Long> tiggerTs;


        @Override
        public void open(Configuration parameters) throws Exception {
            dateList = getRuntimeContext().getListState(new ListStateDescriptor<HotItemCountWithWindowEnd>("dateList", HotItemCountWithWindowEnd.class));
            tiggerTs = getRuntimeContext().getState(new ValueStateDescriptor<Long>("tiggerTs", Long.class));
        }

        @Override
        public void processElement(HotItemCountWithWindowEnd value, Context ctx, Collector<String> out) throws Exception {
            //来一条存一次
            dateList.add(value);

            //不要来一条就注册一个窗口结束时间的定时器，其实设置一个就行了
            if (tiggerTs.value() == null) {
                ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 100L);
                tiggerTs.update(value.getWindowEnd() + 100L);
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器的触发，触发的时候说明该窗口的数据已经全部收集到dateList中去了，然后将数据进行排序输出
            Iterable<HotItemCountWithWindowEnd> dateIters = dateList.get();
            List<HotItemCountWithWindowEnd> list = new ArrayList<>();

            for (HotItemCountWithWindowEnd elem : dateIters) {
                list.add(elem);
            }

            dateList.clear();
            tiggerTs.clear();


            // 排序方式1
//            list.sort(new Comparator<HotItemCountWithWindowEnd>() {
//                @Override
//                public int compare(HotItemCountWithWindowEnd o1, HotItemCountWithWindowEnd o2) {
//                    // 降序 => 后 减 前
//                    return o2.getItemCount().intValue() - o1.getItemCount().intValue();
//                }
//            });

            //排序方式2
            Collections.sort(list);

            StringBuilder resultStr = new StringBuilder();
            resultStr
                    .append("窗口结束时间:" + timestamp + "\n")
                    .append("------------------------------------------------------\n");

            for (int i = 0; i < topN; i++) {
                resultStr.append(list.get(i) + "\n");
            }
            resultStr.append("------------------------------------------------------\n\n");


            out.collect(resultStr.toString());

        }
    }

    public static class CountResultWindowEnd extends ProcessWindowFunction<Long, HotItemCountWithWindowEnd, Long, TimeWindow> {
        @Override
        public void process(Long itemId, Context context, Iterable<Long> elements, Collector<HotItemCountWithWindowEnd> out) throws Exception {
            //这个窗口的数据是聚合之后的数据，也就是这个窗口只有一条数据
            out.collect(new HotItemCountWithWindowEnd(itemId, elements.iterator().next(), context.window().getEnd()));
        }
    }

    public static class AggCount implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior value, Long accumulator) {
            return accumulator + 1L;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return a + b;
        }
    }


}
