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
package org.apache.pig.backend.hadoop.executionengine.spark.operator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;

/**
 * this dummy operator is for RDD.persist (cache)
 */
public class POPersistSpark extends PhysicalOperator {
    private static final long serialVersionUID = 1L;

    public POPersistSpark(OperatorKey k) {
        super(k);
    }

    public POPersistSpark(POPersistSpark copy)
            throws ExecException {
        super(copy);
    }

    @Override
    public Tuple illustratorMarkup(Object in, Object out, int eqClassIndex) {
        return null;
    }

    @Override
    public boolean supportsMultipleInputs() {
        return false;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return true;
    }

    @Override
    public String name() {
        return getAliasString() + "PersistSpark - " + mKey.toString();
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        // v.visitPersistSpark(this);
    }
}
