package com.zky.chapter06;

import com.zky.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

/**
 * @Author Dawn
 * @Date 2021/1/4 15:40
 * @Desc 连续5s水位上升
 */
public class Flink15_ProcessFunction_TimerPractice {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator<WaterSensor> sensorDS = env
                .socketTextStream("hadoop102", 9999)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new WaterSensor(datas[0], Long.valueOf(datas[1]), Integer.valueOf(datas[2]));

                    }
                })
                .assignTimestampsAndWatermarks(
                        new AssignerWithPunctuatedWatermarks<WaterSensor>() {
                            private long maxTs = Long.MIN_VALUE;

                            @Nullable
                            @Override
                            public Watermark checkAndGetNextWatermark(WaterSensor lastElement, long extractedTimestamp) {
                                maxTs = Math.max(maxTs, extractedTimestamp);
                                return new Watermark(maxTs);
                            }

                            @Override
                            public long extractTimestamp(WaterSensor element, long previousElementTimestamp) {
                                return element.getTs() * 1000L;
                            }
                        }
                );

        SingleOutputStreamOperator<String> processDS = sensorDS
                .keyBy(r -> r.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    //定义变量保存上次的水位
                    private Integer lastVc = 0;
                    private Long triggerTs = 0L;

                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                        if (value.getVc() > lastVc) {
                            //水位上升
                            if (triggerTs == 0) {
                                //如果是第一条数据，注册定时器
                                ctx.timerService().registerEventTimeTimer(value.getTs() * 1000L + 5000L);
                                triggerTs = value.getTs() * 1000L + 5000L;
                            }

                        } else {
                            //水位下降，删除定时器
                            ctx.timerService().deleteEventTimeTimer(triggerTs);
                            //并且更新 注册定时器的时间（或 把保存的时间清空）
                            triggerTs = 0L;
                        }

                        // 不管上升还是下降，都要保存水位值，供下条数据使用，进行比较
                        lastVc = value.getVc();
                        System.out.println(ctx.timerService().currentWatermark());
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        // 定时器触发，说明已经满足 连续5s 水位上升
                        out.collect(ctx.getCurrentKey() + "在" + timestamp + "监测到水位连续5s上升");
                        // 将保存的注册时间清空,如果不清空的话，那么如果下一个5s水位一直上升的就报不出来警告了，因为走不到71行的else里面去
                        triggerTs = 0L;
                    }
                });

        processDS.print();

        env.execute();
    }
}
