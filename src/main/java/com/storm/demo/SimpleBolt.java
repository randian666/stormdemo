package com.storm.demo;

import com.alibaba.fastjson.JSON;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @Author: liuxun
 * @CreateDate: 2018/7/16 下午2:18
 * @Version: 1.0
 */
public class SimpleBolt extends BaseRichBolt {
    OutputCollector outputCollector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        System.out.println("初始化配置以及组件："+ JSON.toJSONString(map));
        this.outputCollector=outputCollector;
    }
    @Override
    public void execute(Tuple tuple) {
        try {
            String msg=tuple.getString(0);
            if (msg!=null){
                System.out.println("SimpleBolt 收到消息："+msg);
            }
            outputCollector.ack(tuple);
//            throw new Exception("异常"+tuple.getMessageId());
        } catch (Exception e) {
            outputCollector.fail(tuple);
            System.out.println("消息处理失败 "+tuple.toString());
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
//        outputFieldsDeclarer.declare(new Fields("msg"));
    }
}
