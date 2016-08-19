package org.apache.pig.backend.hadoop.executionengine.spark.converter;


import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POBroadcast;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.Pair;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xiandake on 8/12/16.
 */
public class BroadcastConverter implements RDDConverter<Tuple, List<Tuple>, Tuple, POBroadcast> {

    private final JavaSparkContext sc;

    public BroadcastConverter(JavaSparkContext sc) {
        this.sc = sc;
    }

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
                              Map<String, Broadcast<List<Tuple>>> broadcastedVars,
                              POBroadcast physicalOperator) {
        SparkUtil.assertPredecessorSize(predecessors, physicalOperator, 1);
        RDD<Tuple> smallRDD = predecessors.get(0);

        //JavaSparkContext sparkContext = new JavaSparkContext("","");
        //Broadcast<Tuple[]> broadcastedTuples  = sparkContext.broadcast(smallRDD.collect());

        //Broadcast<Tuple[]> broadcastedTuples = sc.broadcast(smallRDD.collect(), SparkUtil.getManifest(Tuple[].class));

        //TODO:  Map ?

        //Mockup
        TupleFactory tf = TupleFactory.getInstance();

        List<Tuple> mockupDist = new ArrayList<>();
        Map<Object, Pair<Integer, Integer>> reducerMap = new HashMap<>();
        try {
            Tuple t1 = tf.newTuple(2);
            Tuple t2 = tf.newTuple(2);
            t1.set(0, 300);
            t1.set(1, new DataByteArray("strawberry".getBytes()));
            Pair<Integer, Integer> p1 = new Pair<>(0,3);

            t2.set(0, 200);
            t2.set(1, new DataByteArray("orange0001".getBytes()));
            Pair<Integer, Integer> p2 = new Pair<>(4,5);

            reducerMap.put(t1, p1);
            reducerMap.put(t2, p2);

            Tuple t = tf.newTuple(2);
            t.set(0, 7);  //number
            t.set(1, reducerMap);
            mockupDist.add(t);
        } catch (Exception e){

        }

        // smallRDD.toJavaRDD().collect()
        Broadcast<List<Tuple>> broadcastedTuples = sc.broadcast(mockupDist);

        HashMap<String, Broadcast<List<Tuple>>> bcVarsMap = (HashMap<String, Broadcast<List<Tuple>>>) broadcastedVars;
        bcVarsMap.put(physicalOperator.getOperatorKey().toString(), broadcastedTuples);
        //List<Tuple> tuples = broadcastedTuples.value();
        //int s = tuples.size();

        return smallRDD;
    }

}
