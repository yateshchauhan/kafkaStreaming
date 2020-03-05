package com.utility;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class MyPartitioner implements Partitioner {


    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object o1, byte[] valueBytes, Cluster cluster) {

        List<PartitionInfo> infoList = cluster.partitionsForTopic(topic);

        int partitionCount = infoList != null ? infoList.size() : 1;

        int keyHash = Utils.toPositive(Utils.murmur2(keyBytes));

        int partitionNo = keyHash % partitionCount;
        System.out.println("keyHash : "+keyHash+", partition count : "+partitionCount+", Partitions number is : "+partitionNo);
        return partitionNo;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }

    @Override
    public void onNewBatch(String topic, Cluster cluster, int prevPartition) {

    }
}
