package org.apache.pig.backend.hadoop.executionengine.spark.converter;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.PigConfiguration;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POPersistSpark;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;

import java.util.List;
import java.util.Properties;

public class PersistConverter implements RDDConverter<Tuple, Tuple, POPersistSpark> {

    private static Log LOG = LogFactory.getLog(PersistConverter.class);

    private PigContext pigContext;
    public PersistConverter(PigContext pigContext) {
        this.pigContext = pigContext;
    }

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, POPersistSpark po) {

        SparkUtil.assertPredecessorSize(predecessors, po, 1);

        Configuration conf = ConfigurationUtil.toConfiguration(pigContext.getProperties());
        String storageLevel = conf.get(PigConfiguration.PIG_SPARK_PERSIST_STORAGE_LEVEL, "MEMORY_AND_DISK");
        StorageLevel level = null;
        try {
            level = StorageLevel.fromString(storageLevel);
        } catch (Exception e) {
            // if user give a wrong StorageLevel string
            LOG.info(e.getMessage());
        }

        RDD<Tuple> rdd = predecessors.get(0);
        return rdd.persist(level == null ? StorageLevel.MEMORY_AND_DISK() : level);
    }
}
