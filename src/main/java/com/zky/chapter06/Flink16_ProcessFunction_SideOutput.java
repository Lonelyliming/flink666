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
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

/**
 * @Author Dawn
 * @Date 2021/1/4 15:40
 * @Desc 连续5s水位上升
 */
public class Flink16_ProcessFunction_SideOutput {
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
        //TODO 使用侧输出流
        // 1.定义一个OutputTag，给定一个 名称
        // 2.使用 ctx.output(outputTag对象,放入侧输出流的数据)
        // 3.获取侧输出流 => DataStream.getSideOutput(outputTag对象)
        OutputTag<String> outputTag = new OutputTag<String>("vc alarm") {};

        SingleOutputStreamOperator<WaterSensor> processDS = sensorDS
                .keyBy(r -> r.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                        if (value.getVc() > 5){
                            //水位超过阈值，就将超出部分放到侧输出流中去
                            ctx.output(outputTag,"水位高于阈值5！！！");
                        }

                        out.collect(value);
                    }

                });

        //主流
        processDS.print();
        //侧输出流
        processDS.getSideOutput(outputTag).print("alarm");

        env.execute();
    }
}
