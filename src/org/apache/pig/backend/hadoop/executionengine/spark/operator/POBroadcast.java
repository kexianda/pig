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

import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.spark.broadcast.Broadcast;

import java.util.List;
import java.util.Map;


public class POBroadcast extends PhysicalOperator {

    private Map<OperatorKey, Broadcast<List<Tuple>>> broadcastedVarsMap;

    public POBroadcast(OperatorKey k) {
        super(k);
    }

    public POBroadcast(POBroadcast copy)
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
        return false;
    }

    @Override
    public String name() {
        return "Broadcast";
    }

    @Override
    public void visit(PhyPlanVisitor v) throws VisitorException {
        v.visitBroadcast(this);
    }

    public void setBroadcastedVarsMap(Map<OperatorKey,Broadcast<List<Tuple>>> broadcastedVarsMap) {
        this.broadcastedVarsMap = broadcastedVarsMap;
    }

    public Map<OperatorKey,Broadcast<List<Tuple>>> getBroadcastedVarsMap() {
        return this.broadcastedVarsMap;
    }
}
