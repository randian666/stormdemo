package com.storm.demo;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @Author: liuxun
 * @CreateDate: 2018/7/16 下午2:17
 * @Version: 1.0
 */
public class SimpleSpout extends BaseRichSpout {
    //用来发射数据的工具类
    private SpoutOutputCollector collector;
    private static String[] info = new String[]{
            "comaple\t,12424,44w46,654,12424,44w46,654,",
            "lisi\t,435435,6537,12424,44w46,654,",
            "lipeng\t,45735,6757,12424,44w46,654,",
            "hujintao\t,45735,6757,12424,44w46,654,",
            "jiangmin\t,23545,6457,2455,7576,qr44453",
            "beijing\t,435435,6537,12424,44w46,654,",
            "xiaoming\t,46654,8579,w3675,85877,077998,",
            "xiaozhang\t,9789,788,97978,656,345235,09889,",
            "ceo\t,46654,8579,w3675,85877,077998,",
            "cto\t,46654,8579,w3675,85877,077998,",
            "zhansan\t,46654,8579,w3675,85877,077998,"};
    private Random random=new Random();

    @Override
    public void fail(Object msgId) {
        System.out.println("消息处理失败");
        super.fail(msgId);
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("消息处理成功");
        super.ack(msgId);
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector=spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        try {
            String msg=info[random.nextInt(info.length-1)];
            collector.emit(new Values(msg));
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //设置发射数据的字段映射
        outputFieldsDeclarer.declare(new Fields("source"));//collector.emit(new Values(msg));参数要对应
    }
}
