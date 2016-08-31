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
package org.apache.pig.backend.hadoop.executionengine.spark;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigConstants;
import org.apache.pig.PigException;
import org.apache.pig.backend.BackendException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.datastorage.ConfigurationUtil;
import org.apache.pig.backend.hadoop.executionengine.Launcher;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.*;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.spark.converter.*;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POBroadcast;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POGlobalRearrangeSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POReduceBySpark;
import org.apache.pig.backend.hadoop.executionengine.spark.optimizer.AccumulatorOptimizer;
import org.apache.pig.backend.hadoop.executionengine.spark.optimizer.CombinerOptimizer;
import org.apache.pig.backend.hadoop.executionengine.spark.optimizer.MultiQueryOptimizerSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.optimizer.NoopFilterRemover;
import org.apache.pig.backend.hadoop.executionengine.spark.optimizer.ParallelismSetter;
import org.apache.pig.backend.hadoop.executionengine.spark.optimizer.SecondaryKeyOptimizerSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkCompiler;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperPlan;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkOperator;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkPOPackageAnnotator;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkPrinter;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.DotSparkPrinter;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigStats;
import org.apache.pig.tools.pigstats.spark.SparkCounters;
import org.apache.pig.tools.pigstats.spark.SparkPigStats;
import org.apache.pig.tools.pigstats.spark.SparkPigStatusReporter;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.scheduler.JobLogger;
import org.apache.spark.scheduler.StatsReportListener;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * Main class that launches pig for Spark
 */
public class SparkLauncher extends Launcher {

    private static final Log LOG = LogFactory.getLog(SparkLauncher.class);

    // Our connection to Spark. It needs to be static so that it can be reused
    // across jobs, because a
    // new SparkLauncher gets created for each job.
    private static JavaSparkContext sparkContext = null;
    private static JobMetricsListener jobMetricsListener = new JobMetricsListener();
    private String jobGroupID;
    private PigContext pigContext = null;
    private JobConf jobConf = null;
    private String currentDirectoryPath = null;

    @Override
    public PigStats launchPig(PhysicalPlan physicalPlan, String grpName,
                              PigContext pigContext) throws Exception {
        if (LOG.isDebugEnabled())
            LOG.debug(physicalPlan);
        this.pigContext = pigContext;
        initialize();
        SparkOperPlan sparkplan = compile(physicalPlan, pigContext);
        if (LOG.isDebugEnabled())
            explain(sparkplan, System.out, "text", true);
        SparkPigStats sparkStats = (SparkPigStats) pigContext
                .getExecutionEngine().instantiatePigStats();
        sparkStats.initialize(pigContext, sparkplan, jobConf);
        PigStats.start(sparkStats);

        startSparkIfNeeded(pigContext);

        // Set a unique group id for this query, so we can lookup all Spark job
        // ids
        // related to this query.
        jobGroupID = UUID.randomUUID().toString();
        sparkContext.setJobGroup(jobGroupID, "Pig query to Spark cluster",
                false);
        jobMetricsListener.reset();

        this.currentDirectoryPath = Paths.get(".").toAbsolutePath()
                .normalize().toString()
                + "/";
        addFilesToSparkJob();
        LinkedList<POStore> stores = PlanHelper.getPhysicalOperators(
                physicalPlan, POStore.class);
        POStore firstStore = stores.getFirst();
        if (firstStore != null) {
            MapRedUtil.setupStreamingDirsConfSingle(firstStore, pigContext,
                    jobConf);
        }

        new ParallelismSetter(sparkplan, jobConf).visit();

        byte[] confBytes = KryoSerializer.serializeJobConf(jobConf);

        SparkPigStatusReporter.getInstance().setCounters(new SparkCounters(sparkContext));

        // Create conversion map, mapping between pig operator and spark convertor
        Map<Class<? extends PhysicalOperator>, RDDConverter> convertMap
                = new HashMap<Class<? extends PhysicalOperator>, RDDConverter>();
        convertMap.put(POLoad.class, new LoadConverter(pigContext,
                physicalPlan, sparkContext.sc()));
        convertMap.put(POStore.class, new StoreConverter(pigContext));
        convertMap.put(POForEach.class, new ForEachConverter(confBytes));
        convertMap.put(POFilter.class, new FilterConverter(confBytes));
        convertMap.put(POPackage.class, new PackageConverter(confBytes));
        convertMap.put(POLocalRearrange.class, new LocalRearrangeConverter());
        convertMap.put(POGlobalRearrangeSpark.class, new GlobalRearrangeConverter());
        convertMap.put(POLimit.class, new LimitConverter());
        convertMap.put(PODistinct.class, new DistinctConverter());
        convertMap.put(POUnion.class, new UnionConverter(sparkContext.sc()));
        convertMap.put(POSort.class, new SortConverter());
        convertMap.put(POSplit.class, new SplitConverter());
        convertMap.put(POSkewedJoin.class, new SkewedJoinConverter());
        convertMap.put(POMergeJoin.class, new MergeJoinConverter());
        convertMap.put(POCollectedGroup.class, new CollectedGroupConverter());
        convertMap.put(POCounter.class, new CounterConverter());
        convertMap.put(PORank.class, new RankConverter());
        convertMap.put(POStream.class, new StreamConverter(confBytes));
        convertMap.put(POFRJoin.class, new FRJoinConverter());
        convertMap.put(POMergeCogroup.class, new MergeCogroupConverter());
        convertMap.put(POReduceBySpark.class, new ReduceByConverter());
        convertMap.put(POPreCombinerLocalRearrange.class, new LocalRearrangeConverter());
        convertMap.put(POBroadcast.class, new BroadcastConverter(sparkContext));
        convertMap.put(POPoissonSampleSpark.class, new PoissonSampleConverter());

        uploadUDFJars(sparkplan);
        new JobGraphBuilder(sparkplan, convertMap, sparkStats, sparkContext, jobMetricsListener, jobGroupID, jobConf).visit();
        cleanUpSparkJob(sparkStats);
        sparkStats.finish();
        return sparkStats;
    }

    private void uploadUDFJars(SparkOperPlan sparkplan) throws IOException {
        UDFJarsFinder udfJarsFinder = new UDFJarsFinder(sparkplan, pigContext);
        udfJarsFinder.visit();
        Set<String> udfJars = udfJarsFinder.getUdfJars();
        for (String udfJar : udfJars) {
            File jarFile = new File(udfJar);
            addJarToSparkJobWorkingDirectory(jarFile, jarFile.getName());
        }
    }

    private void optimize(PigContext pc, SparkOperPlan plan) throws IOException {

        Configuration conf = ConfigurationUtil.toConfiguration(pc.getProperties());

        // Should be the first optimizer as it introduces new operators to the plan.
        boolean noCombiner = conf.getBoolean(PigConfiguration.PIG_EXEC_NO_COMBINER, false);
        if (!pc.inIllustrator && !noCombiner)  {
            CombinerOptimizer combinerOptimizer = new CombinerOptimizer(plan);
            combinerOptimizer.visit();
            if (LOG.isDebugEnabled()) {
                System.out.println("after combiner optimization:");
                explain(plan, System.out, "text", true);
            }
        }

        boolean noSecondaryKey = conf.getBoolean(PigConfiguration.PIG_EXEC_NO_SECONDARY_KEY, false);
        if (!pc.inIllustrator && !noSecondaryKey) {
            SecondaryKeyOptimizerSpark skOptimizer = new SecondaryKeyOptimizerSpark(plan);
            skOptimizer.visit();
        }

        boolean isAccum = conf.getBoolean("opt.accumulator", true);
        if (isAccum) {
            AccumulatorOptimizer accum = new AccumulatorOptimizer(plan);
            accum.visit();
        }

        // removes the filter(constant(true)) operators introduced by
        // splits.
        NoopFilterRemover fRem = new NoopFilterRemover(plan);
        fRem.visit();

        boolean isMultiQuery = conf.getBoolean(PigConfiguration.PIG_OPT_MULTIQUERY, true);

        if (LOG.isDebugEnabled()) {
            System.out.println("before multiquery optimization:");
            explain(plan, System.out, "text", true);
        }

        if (isMultiQuery) {
            // reduces the number of SparkOpers in the Spark plan generated
            // by multi-query (multi-store) script.
            MultiQueryOptimizerSpark mqOptimizer = new MultiQueryOptimizerSpark(plan);
            mqOptimizer.visit();
        }

        if (LOG.isDebugEnabled()) {
            System.out.println("after multiquery optimization:");
            explain(plan, System.out, "text", true);
        }
    }

    private void cleanUpSparkJob(SparkPigStats sparkStats) throws ExecException {
        LOG.info("clean up Spark Job");
        boolean isLocal = System.getenv("SPARK_MASTER") != null ? System
                .getenv("SPARK_MASTER").equalsIgnoreCase("LOCAL") : true;
        if (isLocal) {
            String shipFiles = pigContext.getProperties().getProperty(
                    "pig.streaming.ship.files");
            if (shipFiles != null) {
                for (String file : shipFiles.split(",")) {
                    File shipFile = new File(file);
                    File deleteFile = new File(currentDirectoryPath + "/"
                            + shipFile.getName());
                    if (deleteFile.exists()) {
                        LOG.info(String.format("delete ship file result: %b",
                                deleteFile.delete()));
                    }
                }
            }
            String cacheFiles = pigContext.getProperties().getProperty(
                    "pig.streaming.cache.files");
            if (cacheFiles != null) {
                for (String file : cacheFiles.split(",")) {
                    String fileName = extractFileName(file.trim());
                    File deleteFile = new File(currentDirectoryPath + "/"
                            + fileName);
                    if (deleteFile.exists()) {
                        LOG.info(String.format("delete cache file result: %b",
                                deleteFile.delete()));
                    }
                }
            }
        }

        // run cleanup for all of the stores
        for (OutputStats output : sparkStats.getOutputStats()) {
            POStore store = output.getPOStore();
            try {
                if (!output.isSuccessful()) {
                    store.getStoreFunc().cleanupOnFailure(
                            store.getSFile().getFileName(),
                            Job.getInstance(output.getConf()));
                } else {
                    store.getStoreFunc().cleanupOnSuccess(
                            store.getSFile().getFileName(),
                            Job.getInstance(output.getConf()));
                }
            } catch (IOException e) {
                throw new ExecException(e);
            } catch (AbstractMethodError nsme) {
                // Just swallow it.  This means we're running against an
                // older instance of a StoreFunc that doesn't implement
                // this method.
            }
        }
    }

    private void addFilesToSparkJob() throws IOException {
        LOG.info("add Files Spark Job");
        String shipFiles = pigContext.getProperties().getProperty(
                "pig.streaming.ship.files");
        shipFiles(shipFiles);
        String cacheFiles = pigContext.getProperties().getProperty(
                "pig.streaming.cache.files");
        cacheFiles(cacheFiles);
    }


    private void shipFiles(String shipFiles)
            throws IOException {
        if (shipFiles != null) {
            for (String file : shipFiles.split(",")) {
                File shipFile = new File(file.trim());
                if (shipFile.exists()) {
                    LOG.info(String.format("shipFile:%s", shipFile));
                    addJarToSparkJobWorkingDirectory(shipFile, shipFile.getName());
                }
            }
        }
    }

    private void cacheFiles(String cacheFiles) throws IOException {
        if (cacheFiles != null) {
            Configuration conf = SparkUtil.newJobConf(pigContext);
            for (String file : cacheFiles.split(",")) {
                String fileName = extractFileName(file.trim());
                Path src = new Path(extractFileUrl(file.trim()));
                File tmpFile = File.createTempFile(fileName, ".tmp");
                Path tmpFilePath = new Path(tmpFile.getAbsolutePath());
                FileSystem fs = tmpFilePath.getFileSystem(conf);
                fs.copyToLocalFile(src, tmpFilePath);
                tmpFile.deleteOnExit();
                LOG.info(String.format("cacheFile:%s", fileName));
                addJarToSparkJobWorkingDirectory(tmpFile, fileName);
            }
        }
    }

    private void addJarToSparkJobWorkingDirectory(File jarFile, String jarName) throws IOException {
        LOG.info("Added jar " + jarName);
        boolean isLocal = System.getenv("SPARK_MASTER") != null ? System
                .getenv("SPARK_MASTER").equalsIgnoreCase("LOCAL") : true;
        if (isLocal) {
            File localFile = new File(currentDirectoryPath + "/"
                    + jarName);
            if (jarFile.getAbsolutePath().equals(localFile.getAbsolutePath())
                    && jarFile.exists()) {
                return;
            }
            // When multiple threads start SparkLauncher, delete/copy actions should be in a critical section
            synchronized(SparkLauncher.class) {
                if (localFile.exists()) {
                    LOG.info(String.format(
                            "jar file %s exists, ready to delete",
                            localFile.getAbsolutePath()));
                    localFile.delete();
                } else {
                    LOG.info(String.format("jar file %s not exists,",
                            localFile.getAbsolutePath()));
                }
                Files.copy(Paths.get(new Path(jarFile.getAbsolutePath()).toString()),
                        Paths.get(localFile.getAbsolutePath()));
            }
        } else {
            sparkContext.addFile(jarFile.toURI().toURL()
                .toExternalForm());
        }
    }

    private String extractFileName(String cacheFileUrl) {
        String[] tmpAry = cacheFileUrl.split("#");
        String fileName = tmpAry != null && tmpAry.length == 2 ? tmpAry[1]
                : null;
        if (fileName == null) {
            throw new RuntimeException("cache file is invalid format, file:"
                    + cacheFileUrl);
        } else {
            LOG.debug("cache file name is valid:" + cacheFileUrl);
            return fileName;
        }
    }

    private String extractFileUrl(String cacheFileUrl) {
        String[] tmpAry = cacheFileUrl.split("#");
        String fileName = tmpAry != null && tmpAry.length == 2 ? tmpAry[0]
                : null;
        if (fileName == null) {
            throw new RuntimeException("cache file is invalid format, file:"
                    + cacheFileUrl);
        } else {
            LOG.debug("cache file name is valid:" + cacheFileUrl);
            return fileName;
        }
    }

    private SparkOperPlan compile(PhysicalPlan physicalPlan,
                                  PigContext pigContext) throws PlanException, IOException,
            VisitorException {
        SparkCompiler sparkCompiler = new SparkCompiler(physicalPlan,
                pigContext);
        sparkCompiler.compile();
        sparkCompiler.connectSoftLink();
        SparkOperPlan sparkPlan = sparkCompiler.getSparkPlan();

        // optimize key - value handling in package
        SparkPOPackageAnnotator pkgAnnotator = new SparkPOPackageAnnotator(
                sparkPlan);
        pkgAnnotator.visit();

        optimize(pigContext, sparkPlan);
        return sparkPlan;
    }

    /**
     * Only one SparkContext may be active per JVM (SPARK-2243). When multiple threads start SparkLaucher,
     * the static member sparkContext should be initialized only once
     */
    private static synchronized void startSparkIfNeeded(PigContext pc) throws PigException {
        if (sparkContext == null) {
            String master = null;
            if (pc.getExecType().isLocal()) {
                master = "local";
            } else {
                master = System.getenv("SPARK_MASTER");
                if (master == null) {
                    LOG.info("SPARK_MASTER not specified, using \"local\"");
                    master = "local";
                }
            }

            String sparkHome = System.getenv("SPARK_HOME");
            String sparkJarsSetting = System.getenv("SPARK_JARS");
            String pigJar = System.getenv("SPARK_PIG_JAR");
            String[] sparkJars = sparkJarsSetting == null ? new String[]{}
                    : sparkJarsSetting.split(",");
            List<String> jars = Lists.asList(pigJar, sparkJars);

            if (!master.startsWith("local") && !master.equals("yarn-client")) {
                // Check that we have the Mesos native library and Spark home
                // are set
                if (sparkHome == null) {
                    System.err
                            .println("You need to set SPARK_HOME to run on a Mesos cluster!");
                    throw new PigException("SPARK_HOME is not set");
                }
            }

            SparkConf sparkConf = new SparkConf();
            Properties pigCtxtProperties = pc.getProperties();

            sparkConf.setMaster(master);
            sparkConf.setAppName("PigOnSpark:" + pigCtxtProperties.getProperty(PigContext.JOB_NAME));
            sparkConf.setJars(jars.toArray(new String[jars.size()]));
            if (sparkHome != null && !sparkHome.isEmpty()) {
                sparkConf.setSparkHome(sparkHome);
            } else {
                LOG.warn("SPARK_HOME is not set");
            }

            //Copy all spark.* properties to SparkConf
            for (String key : pigCtxtProperties.stringPropertyNames()) {
                if (key.startsWith("spark.")) {
                    LOG.debug("Copying key " + key + " with value " +
                        pigCtxtProperties.getProperty(key) + " to SparkConf");
                    sparkConf.set(key, pigCtxtProperties.getProperty(key));
                }
            }

            checkAndConfigureDynamicAllocation(master, sparkConf);

            sparkContext = new JavaSparkContext(sparkConf);
            sparkContext.sc().addSparkListener(new StatsReportListener());
            sparkContext.sc().addSparkListener(new JobLogger());
            sparkContext.sc().addSparkListener(jobMetricsListener);
        }
    }

    private static void checkAndConfigureDynamicAllocation(String master, SparkConf sparkConf) {
        if (sparkConf.getBoolean("spark.dynamicAllocation.enabled", false)) {
            if (!master.startsWith("yarn")) {
                LOG.warn("Dynamic allocation is enabled, but " +
                    "script isn't running on yarn. Ignoring ...");
            }
            if (!sparkConf.getBoolean("spark.shuffle.service.enabled", false)) {
                LOG.info("Spark shuffle service is being enabled as dynamic " +
                    "allocation is enabled");
                sparkConf.set("spark.shuffle.service.enabled", "true");
            }
        }
    }

    // You can use this in unit tests to stop the SparkContext between tests.
    static void stopSpark() {
        if (sparkContext != null) {
            sparkContext.stop();
            sparkContext = null;
        }
    }


    @Override
    public void explain(PhysicalPlan pp, PigContext pc, PrintStream ps,
                        String format, boolean verbose) throws IOException {
        SparkOperPlan sparkPlan = compile(pp, pc);
        explain(sparkPlan, ps, format, verbose);
    }

    private void explain(SparkOperPlan sparkPlan, PrintStream ps,
                         String format, boolean verbose)
            throws IOException {
        Map<OperatorKey, SparkOperator> allOperKeys = sparkPlan.getKeys();
        List<OperatorKey> operKeyList = new ArrayList(allOperKeys.keySet());
        Collections.sort(operKeyList);
        for (OperatorKey operatorKey : operKeyList) {
            SparkOperator op = sparkPlan.getOperator(operatorKey);
            ps.print(op.getOperatorKey());
            List<SparkOperator> successors = sparkPlan.getSuccessors(op);
            if (successors != null) {
                ps.print("->");
                for (SparkOperator suc : successors) {
                    ps.print(suc.getOperatorKey() + " ");
                }
            }
            ps.println();
        }

        if (format.equals("text")) {
            SparkPrinter printer = new SparkPrinter(ps, sparkPlan);
            printer.setVerbose(verbose);
            printer.visit();
        } else if (format.equals("dot")) {
            ps.println("#--------------------------------------------------");
            ps.println("# Spark Plan                                  ");
            ps.println("#--------------------------------------------------");

            DotSparkPrinter printer = new DotSparkPrinter(sparkPlan, ps);
            printer.setVerbose(verbose);
            printer.dump();
            ps.println("");
        } else { // TODO: add support for other file format
            throw new IOException(
                    "Non-text and non-dot output of explain is not supported.");
        }
    }

    @Override
    public void kill() throws BackendException {
        // TODO Auto-generated method stub

    }

    @Override
    public void killJob(String jobID, Configuration conf)
            throws BackendException {
        // TODO Auto-generated method stub

    }

    /**
     * We store the value of udf.import.list in PigContext#properties.getProperty("spark.udf.import.list") in spark mode.
     * Later we will use PigContext#properties.getProperty("spark.udf.import.list")in PigContext#writeObject.
     * we don't save this value in PigContext#properties.getProperty("udf.import.list")
     * because this will cause OOM problem(detailed see PIG-4295).
     */
    private void saveUdfImportList() {
        String udfImportList = Joiner.on(",").join(PigContext.getPackageImportList());
        pigContext.getProperties().setProperty("spark.udf.import.list", udfImportList);
    }

    private void initialize() throws IOException {
        saveUdfImportList();
        jobConf = SparkUtil.newJobConf(pigContext);
        jobConf.set(PigConstants.LOCAL_CODE_DIR,
                System.getProperty("java.io.tmpdir"));

        SchemaTupleBackend.initialize(jobConf, pigContext);
        Utils.setDefaultTimeZone(jobConf);
    }
}
