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
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
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

    private POLocalRearrange[] LRs;
    private POSkewedJoin poSkewedJoin;
//    private final SparkContext sc;
//
//    public SkewedJoinConverter(SparkContext sc) {
//        this.sc = sc;
//    }

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

        // make (key, value) pairs, key has type IndexedKey, value has type Tuple
        RDD<Tuple2<IndexedKey, Tuple>> rdd1Pair = rdd1.map(new ExtractKeyFunction(
                this, 0), SparkUtil.<IndexedKey, Tuple>getTuple2Manifest());
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
                .join(rdd2Pair_javaRDD, new SkewedJoinPartitioner());

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

    private static class SkewedJoinPartitioner extends Partitioner {
        private int parallelism = 5;

        //transient ?
        //todo: reducermap comes from broadcast variable
        protected Map<Tuple, Pair<Integer, Integer>> reducerMap;

        transient private Map<Tuple, Integer> currentIndexMap = Maps.newHashMap();

        @Override
        public int numPartitions() {
            return parallelism;
        }

        @Override
        public int getPartition(Object key) {

            //Broadcast<Map<Tuple, Pair<Integer, Integer>>> distFile;
            //init. to be removed
            reducerMap = Maps.newHashMap();
            //reducerMap.put()

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
