package com.zky.chapter05;

import com.zky.bean.OrderEvent;
import com.zky.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author Dawn
 * @Date 2021/1/1 20:49
 * @Desc 实时对账，将业务系统的订单数据和第三方支付系统的支付数据进行关联起来，如果匹配上说明这个订单支付成功，否则失败
 */
public class Flink30_Case_OrderTxDetect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<OrderEvent> orderDS = env
                .readTextFile("input/OrderLog.csv")
                .map(new MapFunction<String, OrderEvent>() {
                    @Override
                    public OrderEvent map(String value) throws Exception {
                        String[] fields = value.split(",");
                        return new OrderEvent(
                                Long.valueOf(fields[0]),
                                fields[1],
                                fields[2],
                                Long.valueOf(fields[3])
                        );
                    }
                });
        SingleOutputStreamOperator<TxEvent> txDS = env
                .readTextFile("input/ReceiptLog.csv")
                .map(new MapFunction<String, TxEvent>() {
                    @Override
                    public TxEvent map(String value) throws Exception {
                        String[] datas = value.split(",");
                        return new TxEvent(
                                datas[0],
                                datas[1],
                                Long.valueOf(datas[2])
                        );
                    }
                });

        // 2.处理数据：实时对账 监控
        // 两条流 connect 起来，通过 txId 做一个匹配，匹配上就是对账成功
        // 对于同一笔订单的交易来说，业务系统 和 交易系统 的数据，哪个先来，是不一定的
        // TODO 一般两条流connect的时候，会做 keyby，为了要匹配的数据到一起
        // 可以先 keyby再 connect，也可以 先 connect，再 keyby
        KeyedStream<OrderEvent, String> orderKeyDS = orderDS.keyBy(r -> r.getTxId());
        KeyedStream<TxEvent, String> txKeyDS = txDS.keyBy(r -> r.getTxId());

        SingleOutputStreamOperator<String> resultDS = orderKeyDS
                .connect(txKeyDS)
                .process(new CoProcessFunction<OrderEvent, TxEvent, String>() {
                    // 用来存放 交易系统 的数据
                    private Map<String, TxEvent> txMap = new HashMap<>();
                    // 用来存放 业务系统 的数据
                    private Map<String, OrderEvent> orderMap = new HashMap<>();

                    @Override
                    public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                        // 进入这个方法，说明来的数据是 业务系统的数据
                        // 判断 交易数据 来了没有？
                        // 通过 交易码 查询保存的 交易数据 => 如果不为空，说明 交易数据 已经来了，匹配上
                        TxEvent txEvent = txMap.get(value.getTxId());
                        if (txEvent == null) {
                            // 1.说明 交易数据 没来 => 等 ， 把自己临时保存起来
                            orderMap.put(value.getTxId(), value);
                        } else {
                            // 2.说明 交易数据 来了 => 对账成功
                            out.collect("订单" + value.getOrderId() + "对账成功");
                            //性能优化， 对账成功，将保存的 交易数据 删掉
                            txMap.remove(value.getTxId());
                        }
                    }

                    @Override
                    public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                        OrderEvent orderEvent = orderMap.get(value.getTxId());
                        if (orderEvent == null) {
                            txMap.put(value.getTxId(), value);
                        } else {
                            out.collect("订单" + orderEvent.getTxId() + "对账成功");
                            orderMap.remove(value.getTxId());
                        }
                    }
                });

        resultDS.print();

        env.execute();
    }
}
