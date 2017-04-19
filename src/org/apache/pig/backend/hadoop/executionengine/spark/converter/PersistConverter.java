package org.apache.pig.backend.hadoop.executionengine.spark.converter;


import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POPersistSpark;
import org.apache.pig.data.Tuple;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;

import java.util.List;

public class PersistConverter implements RDDConverter<Tuple, Tuple, POPersistSpark> {

    //private final JavaSparkContext sc;

//    public PersistConverter(JavaSparkContext sc) {
//        this.sc = sc;
//    }

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, POPersistSpark po) {

        SparkUtil.assertPredecessorSize(predecessors, po, 1);

        RDD<Tuple> rdd = predecessors.get(0);
        return rdd.persist(StorageLevel.MEMORY_ONLY());  // or StorageLevel comes from conf ?
    }
}
