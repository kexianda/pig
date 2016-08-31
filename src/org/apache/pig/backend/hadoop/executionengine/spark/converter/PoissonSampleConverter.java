package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPoissonSample;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPoissonSampleSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.data.Tuple;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by xiandake on 8/30/16.
 */
public class PoissonSampleConverter implements RDDConverter<Tuple, List<Tuple>,Tuple, POPoissonSampleSpark> {


    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, Map<String, Broadcast<List<Tuple>>> broadcastedVars,
                              POPoissonSampleSpark po) throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, po, 1);
        RDD<Tuple> rdd = predecessors.get(0);
        PoissionSampleFunction poissionSampleFunction = new PoissionSampleFunction(po);
        return rdd.toJavaRDD().mapPartitions(poissionSampleFunction, false).rdd();
    }

    private static class PoissionSampleFunction implements FlatMapFunction<Iterator<Tuple>, Tuple> {

        private final POPoissonSampleSpark po;

        public PoissionSampleFunction(POPoissonSampleSpark po) {
            this.po = po;
        }

        @Override
        public Iterable<Tuple> call(final Iterator<Tuple> tuples) {

            return new Iterable<Tuple>() {

                public Iterator<Tuple> iterator() {
                    return new OutputConsumerIterator(tuples) {

                        @Override
                        protected void attach(Tuple tuple) {
                            po.setInputs(null);
                            po.attachInput(tuple);
                        }

                        @Override
                        protected Result getNextResult() throws ExecException {
                            return po.getNextTuple();
                        }

                        @Override
                        protected void endOfInput() {
                            po.setEndOfInput(true);
                        }
                    };
                }
            };
        }
    }
}
