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

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

import com.google.common.collect.Maps;
import org.apache.pig.impl.io.InterStorage;
import org.apache.pig.impl.util.Pair;
import org.apache.spark.Partitioner;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.runtime.AbstractFunction1;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSkewedJoin;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.util.MultiMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;

public class SkewedJoinConverter implements
        RDDConverter<Tuple, List<Tuple>, Tuple, POSkewedJoin>, Serializable {

    private POLocalRearrange[] LRs;
    private POSkewedJoin poSkewedJoin;

    private String skewedJoinPartitionFile;

    public void setSkewedJoinPartitionFile(String partitionFile) {
        skewedJoinPartitionFile = partitionFile;
    }

//    private final SparkContext sc;
//
//    public SkewedJoinConverter(SparkContext sc) {
//        this.sc = sc;
//    }

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
                              Map<String, Broadcast<List<Tuple>>> broadcastedVars,
                              POSkewedJoin poSkewedJoin) throws IOException {

        SparkUtil.assertPredecessorSize(predecessors, poSkewedJoin, 2);
        LRs = new POLocalRearrange[2];
        this.poSkewedJoin = poSkewedJoin;

        createJoinPlans(poSkewedJoin.getJoinPlans());

        // extract the two RDDs
        RDD<Tuple> rdd1 = predecessors.get(0);
        RDD<Tuple> rdd2 = predecessors.get(1);

        Broadcast<List<Tuple>> distTuples = broadcastedVars.get(skewedJoinPartitionFile);

        ExtractKeyFunctionWithPartitionId extractFunction = new ExtractKeyFunctionWithPartitionId(this, 0, distTuples);
        RDD<Tuple2<IndexedKey, Tuple>> rdd1Pair = rdd1.map(extractFunction,
                SparkUtil.<IndexedKey, Tuple>getTuple2Manifest());

        // make (key, value) pairs, key has type IndexedKey, value has type Tuple
        //RDD<Tuple2<IndexedKey, Tuple>> rdd1Pair = rdd1.map(new ExtractKeyFunction(
        //        this, 0), SparkUtil.<IndexedKey, Tuple>getTuple2Manifest());
        RDD<Tuple2<IndexedKey, Tuple>> rdd2Pair = rdd2.map(new ExtractKeyFunction(
                this, 1), SparkUtil.<IndexedKey, Tuple>getTuple2Manifest());

        // join fn is present in JavaPairRDD class ..
        JavaPairRDD<IndexedKey, Tuple> rdd1Pair_javaRDD = new JavaPairRDD<IndexedKey, Tuple>(
                rdd1Pair, SparkUtil.getManifest(IndexedKey.class),
                SparkUtil.getManifest(Tuple.class));
        JavaPairRDD<IndexedKey, Tuple> rdd2Pair_javaRDD = new JavaPairRDD<IndexedKey, Tuple>(
                rdd2Pair, SparkUtil.getManifest(IndexedKey.class),
                SparkUtil.getManifest(Tuple.class));

        // join() returns (key, (t1, t2)) where (key, t1) is in this and (key, t2) is in other
        JavaPairRDD<IndexedKey, Tuple2<Tuple, Tuple>> result_KeyValue = rdd1Pair_javaRDD
                .join(rdd2Pair_javaRDD, new SkewedJoinPartitioner(distTuples));

        // map to get JavaRDD<Tuple> from JAvaPairRDD<IndexedKey, Tuple2<Tuple, Tuple>> by
        // ignoring the key (of type IndexedKey) and appending the values (the
        // Tuples)
        JavaRDD<Tuple> result = result_KeyValue
                .mapPartitions(new ToValueFunction());

        // return type is RDD<Tuple>, so take it from JavaRDD<Tuple>
        return result.rdd();
    }

    private void createJoinPlans(MultiMap<PhysicalOperator, PhysicalPlan> inpPlans) throws PlanException {

        int i = -1;
        for (PhysicalOperator inpPhyOp : inpPlans.keySet()) {
            ++i;
            POLocalRearrange lr = new POLocalRearrange(genKey());
            try {
                lr.setIndex(i);
            } catch (ExecException e) {
                throw new PlanException(e.getMessage(), e.getErrorCode(), e.getErrorSource(), e);
            }
            lr.setResultType(DataType.TUPLE);
            lr.setKeyType(DataType.TUPLE);//keyTypes.get(i).size() > 1 ? DataType.TUPLE : keyTypes.get(i).get(0));
            lr.setPlans(inpPlans.get(inpPhyOp));
            LRs[i] = lr;
        }
    }

    private OperatorKey genKey() {
        return new OperatorKey(poSkewedJoin.getOperatorKey().scope, NodeIdGenerator.getGenerator().getNextNodeId(poSkewedJoin.getOperatorKey().scope));
    }

    private static class ExtractKeyFunction extends
            AbstractFunction1<Tuple, Tuple2<IndexedKey, Tuple>> implements
            Serializable {

        private final SkewedJoinConverter poSkewedJoin;
        private final int LR_index; // 0 for left table, 1 for right table

        public ExtractKeyFunction(SkewedJoinConverter poSkewedJoin, int LR_index) {
            this.poSkewedJoin = poSkewedJoin;
            this.LR_index = LR_index;
        }

        @Override
        public Tuple2<IndexedKey, Tuple> apply(Tuple tuple) {

            // attach tuple to LocalRearrange
            poSkewedJoin.LRs[LR_index].attachInput(tuple);

            try {
                // getNextTuple() returns the rearranged tuple
                Result lrOut = poSkewedJoin.LRs[LR_index].getNextTuple();

                // If tuple is (AA, 5) and key index is $1, then it lrOut is 0 5
                // (AA), so get(1) returns key
                Byte index = (Byte)((Tuple) lrOut.result).get(0);
                Object key = ((Tuple) lrOut.result).get(1);
                IndexedKey indexedKey = new IndexedKey(index,key);
                Tuple value = tuple;
                // make a (key, value) pair
                Tuple2<IndexedKey, Tuple> tuple_KeyValue = new Tuple2<IndexedKey, Tuple>(
                        indexedKey, value);

                return tuple_KeyValue;

            } catch (Exception e) {
                System.out.print(e);
                return null;
            }
        }

    }

    private static class ExtractKeyFunctionWithPartitionId extends
            AbstractFunction1<Tuple, Tuple2<IndexedKey, Tuple>> implements
            Serializable {

        private final SkewedJoinConverter poSkewedJoin;
        private final int LR_index; // 0 for left table, 1 for right table

        private final Broadcast<List<Tuple>> distTuples;

        private boolean isInit = false;
        protected Map<Tuple, Pair<Integer, Integer>> reducerMap;
        private Integer parallelism = -1;

        private Integer curPartitionId = -1;
        private Map<Tuple, Integer> currentIndexMap = Maps.newHashMap();

        public ExtractKeyFunctionWithPartitionId(SkewedJoinConverter poSkewedJoin, int LR_index,
                                                 Broadcast<List<Tuple>> distTuples) {
            this.poSkewedJoin = poSkewedJoin;
            this.LR_index = LR_index;

            this.distTuples = distTuples;
        }

        @Override
        public Tuple2<IndexedKey, Tuple> apply(Tuple tuple) {
            // attach tuple to LocalRearrange
            poSkewedJoin.LRs[LR_index].attachInput(tuple);

            try {
                TupleFactory tf = TupleFactory.getInstance();

                // getNextTuple() returns the rearranged tuple
                Result lrOut = poSkewedJoin.LRs[LR_index].getNextTuple();

                // If tuple is (AA, 5) and key index is $1, then it lrOut is 0 5
                // (AA), so get(1) returns key
                Byte index = (Byte)((Tuple) lrOut.result).get(0);
                Object key = ((Tuple) lrOut.result).get(1);

                Tuple oldKey = (Tuple)key;

                Tuple keyTuple = tf.newTuple(oldKey.size() + 1);
                keyTuple.set(0, getPartitionId(oldKey));
                for(int i = 0; i < oldKey.size(); i++ ) {
                    keyTuple.set(i+1, oldKey.get(i));
                }

                IndexedKey oldIndexedKey = new IndexedKey(index,key);
                IndexedKey indexedKeyWithPartitionId = new IndexedKey(index, keyTuple);
                Tuple value = tuple;
                // make a (key, value) pair
                Tuple2<IndexedKey, Tuple> tuple_KeyValue = new Tuple2<IndexedKey, Tuple>(
                        oldIndexedKey, //indexedKeyWithPartitionId
                        value);

                return tuple_KeyValue;
                //return oldKey

            } catch (Exception e) {
                System.out.print(e);
                return null;
            }
        }

        private Integer getPartitionId(Tuple key) {
            if (!isInit) {
                try {
                    Tuple distTuple = distTuples.value().get(0);
                    parallelism = (Integer) distTuple.get(0);
                    reducerMap = (Map<Tuple, Pair<Integer, Integer>>) distTuple.get(1);
                } catch (ExecException e) {

                }
            }
            // for partition table, compute the index based on the sampler output
            Pair <Integer, Integer> indexes;
            Integer curIndex = -1;
            Tuple keyTuple = key;

            // if the partition file is empty, use numPartitions
            //
            //
            //
            //
            // = (totalReducers > 0) ? totalReducers : numPartitions;

            indexes = reducerMap.get(keyTuple);
            // if the reducerMap does not contain the key, do the default hash based partitioning
            if (indexes == null) {
                return (Math.abs(keyTuple.hashCode() % parallelism));
            }

            if (currentIndexMap.containsKey(keyTuple)) {
                curIndex = currentIndexMap.get(keyTuple);
            }

            if (curIndex >= (indexes.first + indexes.second) || curIndex == -1) {
                curIndex = indexes.first;
            } else {
                curIndex++;
            }

            // set it in the map
            currentIndexMap.put(keyTuple, curIndex);
            return (curIndex % parallelism);
        }

    }

    private static class ToValueFunction
            implements
            FlatMapFunction<Iterator<Tuple2<IndexedKey, Tuple2<Tuple, Tuple>>>, Tuple>, Serializable {

        private class Tuple2TransformIterable implements Iterable<Tuple> {

            Iterator<Tuple2<IndexedKey, Tuple2<Tuple, Tuple>>> in;

            Tuple2TransformIterable(
                    Iterator<Tuple2<IndexedKey, Tuple2<Tuple, Tuple>>> input) {
                in = input;
            }

            public Iterator<Tuple> iterator() {
                return new IteratorTransform<Tuple2<IndexedKey, Tuple2<Tuple, Tuple>>, Tuple>(
                        in) {
                    @Override
                    protected Tuple transform(
                            Tuple2<IndexedKey, Tuple2<Tuple, Tuple>> next) {
                        try {

                            Tuple leftTuple = next._2._1;
                            Tuple rightTuple = next._2._2;

                            TupleFactory tf = TupleFactory.getInstance();
                            Tuple result = tf.newTuple(leftTuple.size()
                                    + rightTuple.size());

                            // append the two tuples together to make a
                            // resulting tuple
                            for (int i = 0; i < leftTuple.size(); i++)
                                result.set(i, leftTuple.get(i));
                            for (int i = 0; i < rightTuple.size(); i++)
                                result.set(i + leftTuple.size(),
                                        rightTuple.get(i));

                            System.out.println("MJC: Result = "
                                    + result.toDelimitedString(" "));

                            return result;

                        } catch (Exception e) {
                            System.out.println(e);
                        }
                        return null;
                    }
                };
            }
        }

        @Override
        public Iterable<Tuple> call(
                Iterator<Tuple2<IndexedKey, Tuple2<Tuple, Tuple>>> input) {
            return new Tuple2TransformIterable(input);
        }
    }

    private static class CopyStreamTableWithPartitionIdFunction implements FlatMapFunction<Tuple, Tuple>, Serializable {
        private final Broadcast<List<Tuple>> distTuples;

        private boolean isInit = false;
        protected Map<Tuple, Pair<Integer, Integer>> reducerMap;
        private Integer parallelism = 5;

        public CopyStreamTableWithPartitionIdFunction(Broadcast<List<Tuple>> distFile) {
            distTuples = distFile;
        }

        @Override
        public Iterable<Tuple> call(Tuple key) throws Exception {
            if (!isInit) {
                try {
                    Tuple distTuple = distTuples.value().get(0);

                    parallelism = (Integer) distTuple.get(0);
                    reducerMap = (Map<Tuple, Pair<Integer, Integer>>) distTuple.get(1);
                } catch (ExecException e) {

                }
            }

            TupleFactory tf = TupleFactory.getInstance();
            ArrayList<Tuple> l = new ArrayList();

            Pair <Integer, Integer> indexes = reducerMap.get(key);    // first -> min, second ->max

            // For non skewed keys, we set the partition index to be -1
           if (indexes == null) {
                indexes = new Pair <Integer, Integer>(-1,0);
            }

            for (Integer reducerIdx=indexes.first, cnt=0; cnt <= indexes.second; reducerIdx++, cnt++) {
                if (reducerIdx >= parallelism) {
                    reducerIdx = 0;
                }
                Tuple opTuple = tf.newTuple(key.size() + 1);
                opTuple.set(0, t.get(0));
//                // set the partition index
//                opTuple.set(1, reducerIdx.intValue());
//                opTuple.set(2, key);
//                opTuple.set(3, t.get(2));
            }

            return l;
        }
    }

    private static class SkewedJoinPartitioner extends Partitioner {
        private Integer parallelism = 5;
        private boolean isInit = false;

        Broadcast<List<Tuple>> distTuples;
        //transient ?
        //todo: reducermap comes from broadcast variable
        protected Map<Tuple, Pair<Integer, Integer>> reducerMap;

        transient private Map<Tuple, Integer> currentIndexMap = Maps.newHashMap();

        public SkewedJoinPartitioner(Broadcast<List<Tuple>> bv){
            distTuples = bv;
        }

        @Override
        public int numPartitions() {
            return parallelism;
        }

        @Override
        public int getPartition(Object key) {
            if (!isInit){
//                try {
//                    Tuple t = distTuples.value().get(0);
//                    parallelism = (Integer) t.get(0);
//                    reducerMap = (Map<Tuple, Pair<Integer, Integer>>) t.get(1);
//                } catch (ExecException e) {

//                }
            }

            if (key instanceof IndexedKey){
                //my logic
                IndexedKey idxKey = (IndexedKey)key;

                //todo: dispatch tuple to a partition
                //should be same logic as
                //idxKey.getKey()

                //just for testing. passed!
                int code = idxKey.getKey().hashCode() % parallelism;
                if (code >= 0) {
                    return code;
                } else {
                    return code + parallelism;
                }
            } else {
                int code = key.hashCode() % parallelism;
                if (code >= 0) {
                    return code;
                } else {
                    return code + parallelism;
                }
            }
        }


    }
}
