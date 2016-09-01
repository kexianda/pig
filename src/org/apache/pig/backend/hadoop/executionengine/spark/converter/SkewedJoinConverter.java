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

        //poSkewedJoin.getRequestedParallelism();

        Broadcast<List<Tuple>> keyDist = broadcastedVars.get(skewedJoinPartitionFile);

        // parallelism, if not defined in distKey, get default parallelism
        int parallelism = poSkewedJoin.getRequestedParallelism();
        if (keyDist != null && keyDist.value()!=null) {
            try {
                Tuple distTuple = keyDist.value().get(0);
                parallelism = (int) distTuple.get(0);
            } catch (ExecException e) {
            }
        }
        if (parallelism < 0) {
            if (rdd1.context().conf().contains("spark.default.parallelism")) {
                parallelism = rdd1.context().defaultParallelism();
            } else {
                parallelism = rdd1.getPartitions().length; // getNumPartitions
            }
        }

        ExtractKeyFunctionWithPartitionId extraKeyWithPid = new ExtractKeyFunctionWithPartitionId(this, 0, keyDist);
        RDD<Tuple2<IndexedKey, Tuple>> skewPid_Pair = rdd1.map(extraKeyWithPid, SparkUtil.<IndexedKey, Tuple>getTuple2Manifest());
        JavaPairRDD<IndexedKey, Tuple> skewPid_Pair_javaRDD = new JavaPairRDD<IndexedKey, Tuple>(
                skewPid_Pair, SparkUtil.getManifest(IndexedKey.class),
                SparkUtil.getManifest(Tuple.class));


        RDD<Tuple2<IndexedKey, Tuple>> rdd2Pair = rdd2.map(new ExtractKeyFunction(
                this, 1), SparkUtil.<IndexedKey, Tuple>getTuple2Manifest());
        JavaPairRDD<IndexedKey, Tuple> rdd2Pair_javaRDD = new JavaPairRDD<IndexedKey, Tuple>(
                rdd2Pair, SparkUtil.getManifest(IndexedKey.class),
                SparkUtil.getManifest(Tuple.class));


        //rdd1Pair_javaRDD.map(new AppendPartitionIdFun(distTuples));
        JavaPairRDD<IndexedKey, Tuple> streamPid_Pair_javaRDD = new JavaPairRDD<IndexedKey, Tuple>(
                rdd2Pair_javaRDD.flatMap(new StreamRDDFlatMapWithPartitionId(keyDist)).rdd(),
                SparkUtil.getManifest(IndexedKey.class),
                SparkUtil.getManifest(Tuple.class));


        SkewedJoinPartitioner partitioner = new SkewedJoinPartitioner(parallelism);
        JavaPairRDD<IndexedKey, Tuple2<Tuple, Tuple>> result_KeyValue = skewPid_Pair_javaRDD
                .join(streamPid_Pair_javaRDD, partitioner);

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

        private boolean initialized = false;
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

                Tuple origKey = (Tuple)key;

                Tuple keyTuple = tf.newTuple(origKey.size() + 1);
                int partitionId = getPartitionId(origKey);
                keyTuple.set(0, partitionId);
                for(int i = 0; i < origKey.size(); i++ ) {
                    keyTuple.set(i+1, origKey.get(i));
                }

                //IndexedKey oldIndexedKey = new IndexedKey(index, key);
                IndexedKey indexedKeyWithPartitionId = new IndexedKey(index, keyTuple);

                Tuple value =  tf.newTuple(tuple.size() + 1);
                value.set(0, partitionId);
                for(int i = 0; i < tuple.size(); i++ ) {
                    value.set(i+1, tuple.get(i));
                }

                // make a (key, value) pair
                Tuple2<IndexedKey, Tuple> tuple_KeyValue = new Tuple2<IndexedKey, Tuple>(
                        indexedKeyWithPartitionId,
                        value);

                return tuple_KeyValue;
            } catch (Exception e) {
                System.out.print(e);
                return null;
            }
        }

        private Integer getPartitionId(Tuple keyTuple) {
            if (!initialized) {
                try {
                    Tuple distTuple = distTuples.value().get(0);
                    parallelism = (Integer) distTuple.get(0);
                    reducerMap = (Map<Tuple, Pair<Integer, Integer>>) distTuple.get(1);
                } catch (ExecException e) {

                }
                initialized = true;
            }

            //if no pig.keydist, so that the partitioner will do the default hash based partitioning
            if (distTuples == null || reducerMap == null) {
                //return (Math.abs(keyTuple.hashCode() % parallelism));
                return -1;
            }

            // for partition table, compute the index based on the sampler output
            Pair <Integer, Integer> indexes;
            Integer curIndex = -1;

            indexes = reducerMap.get(keyTuple);

            // if the reducerMap does not contain the key
            if (indexes == null) {
                //return (Math.abs(keyTuple.hashCode() % parallelism));
                return -1;
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

    // POPartitionRearrange is not used in spark mode now,
    // we copy the stream table's records and append a partition id here,
    // see: https://wiki.apache.org/pig/PigSkewedJoinSpec
    // then use spark's flatMap
    // with user defined partitioner, we send the copied stream records to work nodes
    private static class StreamRDDFlatMapWithPartitionId implements FlatMapFunction < Tuple2<IndexedKey, Tuple>, Tuple2<IndexedKey, Tuple> > {

        private final Broadcast<List<Tuple>> distTuples;

        private boolean initialized = false;
        protected Map<Tuple, Pair<Integer, Integer>> reducerMap;
        private Integer parallelism;

        public StreamRDDFlatMapWithPartitionId(Broadcast<List<Tuple>> distTuples) {
            this.distTuples = distTuples;
        }

        public Iterable<Tuple2<IndexedKey, Tuple>> call(Tuple2<IndexedKey, Tuple> t) throws Exception {
            if (!initialized) {
                try {
                    Tuple distTuple = distTuples.value().get(0);

                    parallelism = (Integer) distTuple.get(0);
                    reducerMap = (Map<Tuple, Pair<Integer, Integer>>) distTuple.get(1);
                } catch (ExecException e) {

                }
                initialized = true;
            }

            ArrayList<Tuple2<IndexedKey, Tuple>> l = new ArrayList();
            TupleFactory tf = TupleFactory.getInstance();

            Tuple key = (Tuple) t._1.getKey();
            Pair<Integer, Integer> indexes = reducerMap.get(key);

            // For non skewed keys, we set the partition index to be -1
            if (indexes == null) {
                indexes = new Pair<Integer, Integer>(-1, 0);
            }

            Tuple value = t._2;

            for (Integer reducerIdx = indexes.first, cnt = 0; cnt <= indexes.second; reducerIdx++, cnt++) {
                if (reducerIdx >= parallelism) {
                    reducerIdx = 0;
                }
                Tuple newKey = tf.newTuple(key.size() + 1);
                // set the partition index
                newKey.set(0, reducerIdx.intValue());
                for (int i = 0; i < key.size(); i++) {
                    newKey.set(i + 1, key.get(i));
                }
                IndexedKey newIdxKey = new IndexedKey((byte)1, newKey);

                Tuple newValue = tf.newTuple(value.size() + 1);
                newValue.set(0, reducerIdx.intValue());
                for (int i = 0; i < value.size(); i++) {
                    newValue.set(i + 1, value.get(i));
                }

                l.add(new Tuple2(newIdxKey, newValue));
            }

            return  l;
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
                                    + rightTuple.size() - 2);  // strip the first field, which is the partition id

                            // append the two tuples together to make a
                            // resulting tuple
                            // strip the first field, which is the partition id
                            for (int i = 1; i < leftTuple.size(); i++)
                                result.set(i-1, leftTuple.get(i));
                            for (int i = 1; i < rightTuple.size(); i++)
                                result.set((i - 1) + (leftTuple.size() - 1),
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

    private static class SkewedJoinPartitioner extends Partitioner {
        private int numPartitions;
        public SkewedJoinPartitioner(int parallism) {
            numPartitions = parallism;
        }
        @Override
        public int numPartitions() {
            return numPartitions;
        }

        @Override
        public int getPartition(Object key) {
            try {
                if (key instanceof IndexedKey) {

                    Tuple keyTuple = (Tuple) ((IndexedKey) key).getKey();
                    int partitionId = (int) keyTuple.get(0);
                    if (partitionId >= 0) {
                        return partitionId;
                    }
                }

                //else: default using hashcode
                {
                    Tuple keyTuple = (Tuple) ((IndexedKey) key).getKey();
                    TupleFactory tf = TupleFactory.getInstance();
                    Tuple origKey = tf.newTuple(keyTuple.size() - 1);
                    for (int i = 1; i < keyTuple.size(); i++) {
                        origKey.set(i-1, keyTuple.get(i));
                    }

                    int code = origKey.hashCode() % numPartitions;
                    if (code >= 0) {
                        return code;
                    } else {
                        return code + numPartitions;
                    }
                }
            }
            catch (ExecException e) {
                System.out.println(e);
            }
            return 0;
        }

    }

    private int getParamlism(Broadcast<List<Tuple>> keyDist) {
        //rdd1.context().conf().contains("spark.default.parallelism");
        //rdd.context.defaultParallelism
        //parallelism = rdd1.getPartitions().length; // getNumPartitions
        int parallelism = -1;
        try {
            Tuple distTuple = keyDist.value().get(0);
            parallelism = (int) distTuple.get(0);
        } catch (ExecException e) {

        }

        return parallelism;
    }
}
