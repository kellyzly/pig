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
package org.apache.pig.backend.hadoop.executionengine.spark.plan;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.MultiMap;

/**
 * An operator model for a Spark job. Acts as a host to the plans that will
 * execute in spark.
 */
public class SparkOperator extends Operator<SparkOpPlanVisitor> {
    private static enum OPER_FEATURE {
        NONE,
        // Indicate if this job is a sampling job
        SAMPLER,
        // Indicate if this job is a merge indexer
        INDEXER,
        // Indicate if this job is a group by job
        GROUPBY,
        // Indicate if this job is a cogroup job
        COGROUP,
        // Indicate if this job is a regular join job
        HASHJOIN,
        // Indicate if this job is a union job
        UNION,
        // Indicate if this job is a native job
        NATIVE,
        // Indicate if this job is a limit job
        LIMIT,
        // Indicate if this job is a limit job after sort
        LIMIT_AFTER_SORT;
    };

    public PhysicalPlan physicalPlan;

    public Set<String> UDFs;

    /* Name of the Custom Partitioner used */
    public String customPartitioner = null;

    public Set<PhysicalOperator> scalars;

    public int requestedParallelism = -1;

    private BitSet feature = new BitSet();

    private boolean splitter = false;

    // Name of the partition file generated by sampling process,
    // Used by Skewed Join
    private String skewedJoinPartitionFile;

    private boolean usingTypedComparator = false;

    private boolean combineSmallSplits = true;

    private List<String> crossKeys = null;

    private MultiMap<OperatorKey, OperatorKey> multiQueryOptimizeConnectionMap = new MultiMap<OperatorKey, OperatorKey>();

    // Indicates if a UDF comparator is used
    boolean isUDFComparatorUsed = false;

    //The quantiles file name if globalSort is true
    private String quantFile;

    //Indicates if this job is an order by job
    private boolean globalSort = false;

    public SparkOperator(OperatorKey k) {
        super(k);
        physicalPlan = new PhysicalPlan();
        UDFs = new HashSet<String>();
        scalars = new HashSet<PhysicalOperator>();
    }

    @Override
    public boolean supportsMultipleInputs() {
        return true;
    }

    @Override
    public boolean supportsMultipleOutputs() {
        return true;
    }

    @Override
    public String name() {
        String udfStr = getUDFsAsStr();
        StringBuilder sb = new StringBuilder("Spark" + "("
                + requestedParallelism + (udfStr.equals("") ? "" : ",")
                + udfStr + ")" + " - " + mKey.toString());
        return sb.toString();
    }

    private String getUDFsAsStr() {
        StringBuilder sb = new StringBuilder();
        if (UDFs != null && UDFs.size() > 0) {
            for (String str : UDFs) {
                sb.append(str.substring(str.lastIndexOf('.') + 1));
                sb.append(',');
            }
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    public void add(PhysicalOperator physicalOper) {
        this.physicalPlan.add(physicalOper);
    }

    @Override
    public void visit(SparkOpPlanVisitor v) throws VisitorException {
        v.visitSparkOp(this);
    }

    public void addCrossKey(String key) {
        if (crossKeys == null) {
            crossKeys = new ArrayList<String>();
        }
        crossKeys.add(key);
    }

    public List<String> getCrossKeys() {
        return crossKeys;
    }

    public boolean isGroupBy() {
        return feature.get(OPER_FEATURE.GROUPBY.ordinal());
    }

    public void markGroupBy() {
        feature.set(OPER_FEATURE.GROUPBY.ordinal());
    }

    public boolean isCogroup() {
        return feature.get(OPER_FEATURE.COGROUP.ordinal());
    }

    public void markCogroup() {
        feature.set(OPER_FEATURE.COGROUP.ordinal());
    }

    public boolean isRegularJoin() {
        return feature.get(OPER_FEATURE.HASHJOIN.ordinal());
    }

    public void markRegularJoin() {
        feature.set(OPER_FEATURE.HASHJOIN.ordinal());
    }

    public int getRequestedParallelism() {
        return requestedParallelism;
    }

    public void setSplitter(boolean spl) {
        splitter = spl;
    }

    public boolean isSplitter() {
        return splitter;
    }

    public boolean isSampler() {
        return feature.get(OPER_FEATURE.SAMPLER.ordinal());
    }

    public void markSampler() {
        feature.set(OPER_FEATURE.SAMPLER.ordinal());
    }

    public void setSkewedJoinPartitionFile(String file) {
        skewedJoinPartitionFile = file;
    }

    public String getSkewedJoinPartitionFile() {
        return skewedJoinPartitionFile;
    }

    protected boolean usingTypedComparator() {
        return usingTypedComparator;
    }

    protected void useTypedComparator(boolean useTypedComparator) {
        this.usingTypedComparator = useTypedComparator;
    }

    protected void noCombineSmallSplits() {
        combineSmallSplits = false;
    }

    public boolean combineSmallSplits() {
        return combineSmallSplits;
    }

    public boolean isIndexer() {
        return feature.get(OPER_FEATURE.INDEXER.ordinal());
    }

    public void markIndexer() {
        feature.set(OPER_FEATURE.INDEXER.ordinal());
    }
    public boolean isUnion() {
        return feature.get(OPER_FEATURE.UNION.ordinal());
    }

    public void markUnion() {
        feature.set(OPER_FEATURE.UNION.ordinal());
    }

    public boolean isNative() {
        return feature.get(OPER_FEATURE.NATIVE.ordinal());
    }

    public void markNative() {
        feature.set(OPER_FEATURE.NATIVE.ordinal());
    }

    public boolean isLimit() {
        return feature.get(OPER_FEATURE.LIMIT.ordinal());
    }

    public void markLimit() {
        feature.set(OPER_FEATURE.LIMIT.ordinal());
    }

    public boolean isLimitAfterSort() {
        return feature.get(OPER_FEATURE.LIMIT_AFTER_SORT.ordinal());
    }

    public void markLimitAfterSort() {
        feature.set(OPER_FEATURE.LIMIT_AFTER_SORT.ordinal());
    }

    public void copyFeatures(SparkOperator copyFrom, List<OPER_FEATURE> excludeFeatures) {
        for (OPER_FEATURE opf : OPER_FEATURE.values()) {
            if (excludeFeatures != null && excludeFeatures.contains(opf)) {
                continue;
            }
            if (copyFrom.feature.get(opf.ordinal())) {
                feature.set(opf.ordinal());
            }
        }
    }

	public boolean isSkewedJoin() {
		return (skewedJoinPartitionFile != null);
	}

    public void setRequestedParallelism(int requestedParallelism) {
        this.requestedParallelism = requestedParallelism;
    }

    public void setRequestedParallelismByReference(SparkOperator oper) {
        this.requestedParallelism = oper.requestedParallelism;
    }

    //If enable multiquery optimizer, in some cases, the predecessor(from) of a physicalOp(to) will be the leaf physicalOperator of
    //previous sparkOperator.More detail see PIG-4675
    public void addMultiQueryOptimizeConnectionItem(OperatorKey to, OperatorKey from) {
        multiQueryOptimizeConnectionMap.put(to, from);
    }

    public MultiMap<OperatorKey, OperatorKey> getMultiQueryOptimizeConnectionItem() {
        return multiQueryOptimizeConnectionMap;
    }

    public void setGlobalSort(boolean globalSort) {
        this.globalSort = globalSort;
    }

    public boolean isGlobalSort() {
        return globalSort;
    }

}
