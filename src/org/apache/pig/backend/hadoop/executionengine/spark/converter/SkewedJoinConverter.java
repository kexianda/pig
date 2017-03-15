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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import java.util.Map;

import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.impl.builtin.PartitionSkewedKeys;
import org.apache.pig.impl.util.Pair;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkContext;
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
        RDDConverter<Tuple, Tuple, POSkewedJoin>, Serializable {

    private static Log log = LogFactory.getLog(SkewedJoinConverter.class);

    private POLocalRearrange[] LRs;
    private POSkewedJoin poSkewedJoin;

    private String skewedJoinPartitionFile;
    public void setSkewedJoinPartitionFile(String partitionFile) {
        skewedJoinPartitionFile = partitionFile;
    }

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors,
                              POSkewedJoin poSkewedJoin) throws IOException {

        SparkUtil.assertPredecessorSize(predecessors, poSkewedJoin, 2);
        LRs = new POLocalRearrange[2];
        this.poSkewedJoin = poSkewedJoin;

        createJoinPlans(poSkewedJoin.getJoinPlans());

        // extract the two RDDs
        RDD<Tuple> rdd1 = predecessors.get(0);
        RDD<Tuple> rdd2 = predecessors.get(1);


        Broadcast<List<Tuple>> keyDist = SparkUtil.getBroadcastedVars().get(skewedJoinPartitionFile);

        // if no keyDist,  ExtractKeyAndAppendPidFunction and CopyStreamWithPidFunction need  defaultParallelism
        Integer defaultParallelism =  SparkUtil.getParallelism(predecessors, poSkewedJoin);

        ExtractKeyAndAppendPidFunction extraKeyAppendPid = new ExtractKeyAndAppendPidFunction(this, 0, keyDist, defaultParallelism);
        RDD<Tuple2<IndexedKey, Tuple>> skewRDDWithPid_Pair = rdd1.map(extraKeyAppendPid, SparkUtil.<IndexedKey, Tuple>getTuple2Manifest());
        JavaPairRDD<IndexedKey, Tuple> skewRDDWithPid_Pair_javaRDD = new JavaPairRDD<IndexedKey, Tuple>(
                skewRDDWithPid_Pair, SparkUtil.getManifest(IndexedKey.class),
                SparkUtil.getManifest(Tuple.class));


        RDD<Tuple2<IndexedKey, Tuple>> rdd2Pair = rdd2.map(new ExtractKeyFunction(
                this, 1), SparkUtil.<IndexedKey, Tuple>getTuple2Manifest());
        JavaPairRDD<IndexedKey, Tuple> rdd2Pair_javaRDD = new JavaPairRDD<IndexedKey, Tuple>(
                rdd2Pair, SparkUtil.getManifest(IndexedKey.class),
                SparkUtil.getManifest(Tuple.class));


        CopyStreamWithPidFunction copyStreamWithPidFunction = new CopyStreamWithPidFunction(keyDist, defaultParallelism);
        JavaPairRDD<IndexedKey, Tuple> streamRDDWithPid_Pair_javaRDD = new JavaPairRDD<IndexedKey, Tuple>(
                rdd2Pair_javaRDD.flatMap(copyStreamWithPidFunction).rdd(),
                SparkUtil.getManifest(IndexedKey.class),
                SparkUtil.getManifest(Tuple.class));


        SkewedJoinPartitioner partitioner = buildPartitioner(keyDist, defaultParallelism);
        JavaPairRDD<IndexedKey, Tuple2<Tuple, Tuple>> result_KeyValue = skewRDDWithPid_Pair_javaRDD
                .join(streamRDDWithPid_Pair_javaRDD, partitioner);

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

    /**
     * append a Partition id to the records from skewed table.
     * so that the SkewedJoinPartitioner can send skewed records to different reducer
     *
     * see: https://wiki.apache.org/pig/PigSkewedJoinSpec
     */
    private static class ExtractKeyAndAppendPidFunction extends
            AbstractFunction1<Tuple, Tuple2<IndexedKey, Tuple>> implements
            Serializable {

        private final SkewedJoinConverter poSkewedJoin;
        private final int LR_index; // 0 for left table, 1 for right table

        private final Broadcast<List<Tuple>> keyDist;
        private final Integer defaultParallelism;

        transient private boolean initialized = false;
        transient protected Map<Tuple, Pair<Integer, Integer>> reducerMap;
        transient private Integer parallelism = -1;
        transient private Map<Tuple, Integer> currentIndexMap;

        public ExtractKeyAndAppendPidFunction(SkewedJoinConverter poSkewedJoin, int LR_index,
                                              Broadcast<List<Tuple>> keyDist,
                                              Integer defaultParallelism) {
            this.poSkewedJoin = poSkewedJoin;
            this.LR_index = LR_index;

            this.keyDist = keyDist;
            this.defaultParallelism = defaultParallelism;
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
                Integer[] reducers = new Integer[1];
                reducerMap = loadKeyDistribution(keyDist, reducers);
                parallelism = reducers[0];

                if (parallelism <= 0) {
                    parallelism = defaultParallelism;
                }

                currentIndexMap = Maps.newHashMap();

                initialized = true;
            }

            // for partition table, compute the index based on the sampler output
            Pair <Integer, Integer> indexes;
            Integer curIndex = -1;

            indexes = reducerMap.get(keyTuple);

            // if the reducerMap does not contain the key return -1 so that the
            // partitioner will do the default hash based partitioning
            if (indexes == null) {
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

    /**
     * POPartitionRearrange is not used in spark mode now,
     * Here, use flatMap and CopyStreamWithPidFunction to copy the
     * stream records to the multiple reducers
     *
     * see: https://wiki.apache.org/pig/PigSkewedJoinSpec
     */
    private static class CopyStreamWithPidFunction implements FlatMapFunction < Tuple2<IndexedKey, Tuple>, Tuple2<IndexedKey, Tuple> > {

        private final Broadcast<List<Tuple>> keyDist;
        private final Integer defaultParallelism;

        private transient  boolean initialized = false;
        protected transient Map<Tuple, Pair<Integer, Integer>> reducerMap;
        private transient Integer parallelism;

        public CopyStreamWithPidFunction(Broadcast<List<Tuple>> keyDist, Integer defaultParallelism) {
            this.keyDist = keyDist;
            this.defaultParallelism = defaultParallelism;
        }

        public Iterable<Tuple2<IndexedKey, Tuple>> call(Tuple2<IndexedKey, Tuple> t) throws Exception {
            if (!initialized) {
                Integer[] reducers = new Integer[1];
                reducerMap = loadKeyDistribution(keyDist, reducers);
                parallelism = reducers[0];
                if (parallelism <= 0) {
                    parallelism = defaultParallelism;
                }
                initialized = true;
            }

            ArrayList<Tuple2<IndexedKey, Tuple>> l = new ArrayList();
            TupleFactory tf = TupleFactory.getInstance();

            Tuple key = (Tuple) t._1.getKey();
            Pair<Integer, Integer> indexes = reducerMap.get(key);

            // For non skewed keys, we set the partition index to be -1
            // so that the partitioner will do the default hash based partitioning
            if (indexes == null) {
                indexes = new Pair<>(-1, 0);
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

    /**
     * user defined spark partitioner for skewed join
     */
    private static class SkewedJoinPartitioner extends Partitioner {
        private int numPartitions;
        public SkewedJoinPartitioner(int parallelism) {
            numPartitions = parallelism;
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

                //else: by default using hashcode
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
            catch (ExecException e) {
                System.out.println(e);
            }
            return -1;
        }

    }

    /**
     * use parallelism from keyDist or the default parallelism to
     * create user defined partitioner
     *
     * @param keyDist
     * @param defaultParallelism
     * @return
     */
    private SkewedJoinPartitioner buildPartitioner(Broadcast<List<Tuple>> keyDist, Integer defaultParallelism) {
        Integer parallelism = -1;
        Integer[] reducers = new Integer[1];
        loadKeyDistribution(keyDist, reducers);
        parallelism = reducers[0];
        if (parallelism <= 0) {
            parallelism = defaultParallelism;
        }

        return new SkewedJoinPartitioner(parallelism);
    }

    /**
     * Utility function.
     * 1. Get parallelism
     * 2. build a key distribution map from the broadcasted key distribution file
     * @param keyDist
     * @param totalReducers
     * @return
     */
    private static Map<Tuple, Pair<Integer, Integer>> loadKeyDistribution(Broadcast<List<Tuple>> keyDist,
                                                                          Integer[] totalReducers) {
        Map<Tuple, Pair<Integer, Integer>> reducerMap = new HashMap<>();
        totalReducers[0] = -1; // set a default value

        if (keyDist == null || keyDist.value() == null || keyDist.value().size() == 0) {
            // this could happen if sampling is empty
            log.warn("Empty dist file: ");
            return reducerMap;
        }

        try {
            final TupleFactory tf = TupleFactory.getInstance();

            Tuple t = keyDist.value().get(0);

            Map<String, Object > distMap = (Map<String, Object>) t.get (0);
            DataBag partitionList = (DataBag) distMap.get(PartitionSkewedKeys.PARTITION_LIST);

            totalReducers[0] = Integer.valueOf(""+distMap.get(PartitionSkewedKeys.TOTAL_REDUCERS));

            Iterator<Tuple> it = partitionList.iterator();
            while (it.hasNext()) {
                Tuple idxTuple = it.next();
                Integer maxIndex = (Integer) idxTuple.get(idxTuple.size() - 1);
                Integer minIndex = (Integer) idxTuple.get(idxTuple.size() - 2);
                // Used to replace the maxIndex with the number of reducers
                if (maxIndex < minIndex) {
                    maxIndex = totalReducers[0] + maxIndex;
                }

                // remove the last 2 fields of the tuple, i.e: minIndex and maxIndex and store
                // it in the reducer map
                Tuple keyTuple = tf.newTuple();
                for (int i = 0; i < idxTuple.size() - 2; i++) {
                    keyTuple.append(idxTuple.get(i));
                }

                // number of reducers
                Integer cnt = maxIndex - minIndex;
                reducerMap.put(keyTuple, new Pair(minIndex, cnt));
            }

        } catch (ExecException e) {
            log.warn(e.getMessage());
        }

        return reducerMap;
    }

}
