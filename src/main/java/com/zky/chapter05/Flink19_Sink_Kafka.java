package com.zky.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * @author：Dawn
 * @date：2020/11/27 10:41
 * @Desc：
 */
public class Flink19_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputDS = env.readTextFile("input/sensor-data.log");

        //输出到kafka中去
        inputDS.addSink(new FlinkKafkaProducer011<String>(
                "hadoop102:9092",
                "sensor0421",
                new SimpleStringSchema()
        ));

        env.execute();
    }
}
