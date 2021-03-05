package com.zky.chapter05;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;


import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @author：Dawn
 * @date：2020/11/27 10:41
 * @Desc：
 */
public class Flink20_Sink_Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> inputDS = env.readTextFile("input/sensor-data.log");

        //输出到Redis中去
        FlinkJedisPoolConfig jedisConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .build();

        inputDS.addSink(
                new RedisSink<String>(
                        jedisConfig,
                        new RedisMapper<String>() {
                            // redis 的命令: key是最外层的 key
                            @Override
                            public RedisCommandDescription getCommandDescription() {
                                return new RedisCommandDescription(RedisCommand.HSET, "sensor0421");
                            }

                            //hash的类型，这个指定的是hash的key
                            @Override
                            public String getKeyFromData(String data) {
                                String[] datas = data.split(",");
                                return datas[1];
                            }

                            //hash类型，这个指定的是hash的value
                            @Override
                            public String getValueFromData(String data) {
                                String[] datas = data.split(",");
                                return datas[2];
                            }
                        }
                )
        );

        env.execute();
    }
}
