package org.apache.pig.backend.hadoop.executionengine.spark.converter;


import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POBroadcast;
import org.apache.pig.data.Tuple;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;

import java.util.List;

/**
 * Created by xiandake on 8/12/16.
 */
public class BroadcastConverter implements RDDConverter<Tuple, Tuple, POBroadcast> {

    private final JavaSparkContext sc;

    public BroadcastConverter(JavaSparkContext sc) {
        this.sc = sc;
    }

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
                              POBroadcast physicalOperator) {
        SparkUtil.assertPredecessorSize(predecessors, physicalOperator, 1);
        RDD<Tuple> smallRDD = predecessors.get(0);

        //JavaSparkContext sparkContext = new JavaSparkContext("","");
        //Broadcast<Tuple[]> broadcastedTuples  = sparkContext.broadcast(smallRDD.collect());

        //Broadcast<Tuple[]> broadcastedTuples = sc.broadcast(smallRDD.collect(), SparkUtil.getManifest(Tuple[].class));

        Broadcast<List<Tuple>> broadcastedTuples = sc.broadcast(smallRDD.toJavaRDD().collect());

        return smallRDD;
    }

}
