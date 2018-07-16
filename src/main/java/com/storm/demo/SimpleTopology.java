package com.storm.demo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

/**
 * 启动storm的topology
 * @Author: liuxun
 * @CreateDate: 2018/7/16 下午2:16
 * @Version: 1.0
 */
public class SimpleTopology {
    public static void main(String[] args) {
        try {
            // 实例化TopologyBuilder类。
            TopologyBuilder topologyBuilder = new TopologyBuilder();
            // 设置spout并分配并发数，该并发数将会控制该对象在集群中的线程数。
            topologyBuilder.setSpout("SimpleSpout", new SimpleSpout(), 1);
            // 设置数据处理节点并分配并发数。指定该节点接收喷发节点的策略为随机方式。
            topologyBuilder.setBolt("SimpleBolt", new SimpleBolt(), 3).shuffleGrouping("SimpleSpout");
            //设置配置文件 该配置会下发到spout、bolt中去
            Config config = new Config();
            config.put("jidmb","jim//12345667");
            config.setDebug(false);

            // 这里是本地模式下运行的启动代码。
            config.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("simple", config, topologyBuilder.createTopology());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
