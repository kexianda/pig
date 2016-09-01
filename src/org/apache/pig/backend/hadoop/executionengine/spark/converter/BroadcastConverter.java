/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.pig.backend.hadoop.executionengine.spark.converter;


import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POBroadcast;
import org.apache.pig.data.Tuple;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;

import java.util.List;
import java.util.Map;


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
        RDD<Tuple> rdd = predecessors.get(0);

//        //Mockup
//        TupleFactory tf = TupleFactory.getInstance();
//
//        List<Tuple> mockupDist = new ArrayList<>();
//        Map<Object, Pair<Integer, Integer>> reducerMap = new HashMap<>();
//        try {
//            Tuple t1 = tf.newTuple(2);
//            Tuple t2 = tf.newTuple(2);
//            t1.set(0, 300);
//            t1.set(1, new DataByteArray("strawberry".getBytes()));
//            Pair<Integer, Integer> p1 = new Pair<>(0,3);
//
//            t2.set(0, 200);
//            t2.set(1, new DataByteArray("orange0001".getBytes()));
//            Pair<Integer, Integer> p2 = new Pair<>(4,5);
//
//            reducerMap.put(t1, p1);
//            reducerMap.put(t2, p2);
//
//            Tuple t = tf.newTuple(2);
//            t.set(0, 6);  //number
//            t.set(1, reducerMap);
//            mockupDist.add(t);
//        } catch (Exception e){
//
//        }
//
//        // rdd.toJavaRDD().collect()
//        Broadcast<List<Tuple>> broadcastedTuples = sc.broadcast(mockupDist);
//
//        HashMap<String, Broadcast<List<Tuple>>> bcVarsMap = (HashMap<String, Broadcast<List<Tuple>>>) broadcastedVars;
//        bcVarsMap.put(physicalOperator.getOperatorKey().toString(), broadcastedTuples);
//        //List<Tuple> tuples = broadcastedTuples.value();
//        //int s = tuples.size();


        JavaRDD<Tuple> javaRDD = new JavaRDD<>(rdd, SparkUtil.getManifest(Tuple.class));
        Broadcast<List<Tuple>> broadcastedRDD  = sc.broadcast(javaRDD.collect());

        // use operatorKey as broadcast variable's key
        broadcastedVars.put(physicalOperator.getOperatorKey().toString(), broadcastedRDD);

        // for debugging
        List<Tuple> val = broadcastedRDD.value();

        return rdd;
    }

}
