package com.storm.demo;

import com.alibaba.fastjson.JSON;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @Author: liuxun
 * @CreateDate: 2018/7/16 下午2:18
 * @Version: 1.0
 */
public class SimpleBolt extends BaseBasicBolt {
    /**
     * 初始化bolt
     * @param stormConf
     * @param context
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        System.out.println("初始化配置以及组件："+ JSON.toJSONString(stormConf));
        super.prepare(stormConf, context);
    }

    /**
     * 处理消息流并发射消息
     * @param tuple
     * @param basicOutputCollector
     */
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            String msg=tuple.getString(0);
            if (msg!=null){
                //System.out.println("msg="+msg);
                //处理该消息的逻辑
                basicOutputCollector.emit(new Values(msg + "msg is processed!"));
            }
    }

    /**
     * 定义消息
     * @param outputFieldsDeclarer
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("info"));
    }
}
