package com.zky.chapter05;

import com.zky.bean.WaterSensor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author：Dawn
 * @date：2020/11/26 16:16
 * @Desc：
 */
public class Flink10_Transform_KeyBy {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        DataStreamSource<String> fileDS = env.readTextFile("input/sensor-data.log");

        SingleOutputStreamOperator<WaterSensor> sensorDS = fileDS.map(new Flink06_Transform_Map.MyMapFunction());

        // TODO Keyby:分组
        // 通过 位置索引 或 字段名称 ，返回 Key的类型，无法确定，所以会返回 Tuple，后续使用key的时候，很麻烦
        // 通过 明确的指定 key 的方式， 获取到的 key就是具体的类型 => 实现 KeySelector 或 lambda
        // 分组是逻辑上的分组，即 给每个数据打上标签（属于哪个分组），并不是对并行度进行改变

//        KeyedStream<WaterSensor, Tuple> keyedDS = sensorDS.keyBy(0);
//        KeyedStream<WaterSensor, Tuple> keyedDS = sensorDS.keyBy("id");
//        KeyedStream<WaterSensor, String> keyedDS = sensorDS.keyBy(new MykeySelector());
        KeyedStream<WaterSensor, String> keyedDS = sensorDS.keyBy(r -> r.getId());

    /*    sensorDS.keyBy(0)
                .process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {

                    }
                });

        sensorDS.keyBy("id")
                .process(new KeyedProcessFunction<Tuple, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {

                    }
                });

        sensorDS.keyBy(r -> r.getId())
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {

                    }
                });*/


        fileDS.print();

        env.execute();
    }

    public static class  MykeySelector implements KeySelector<WaterSensor,String>{
        @Override
        public String getKey(WaterSensor value) throws Exception {
            return value.getId();
        }
    }
}
