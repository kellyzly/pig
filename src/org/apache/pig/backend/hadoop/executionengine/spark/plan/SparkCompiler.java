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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.pig.CollectableLoadFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.LoadFunc;
import org.apache.pig.PigConfiguration;
import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.ScalarPhyFinder;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.UDFFinder;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.PhysicalOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POUserFunc;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhyPlanVisitor;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.LitePackager;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCollectedGroup;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POCross;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PODistinct;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFRJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POFilter;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POForEach;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POGlobalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLimit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLoad;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POMergeJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.PONative;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POPackage;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POReservoirSample;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSkewedJoin;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSort;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POSplit;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStream;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POUnion;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.Packager;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.util.PlanHelper;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.NativeSparkOperator;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POLocalRearrangeSpark;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POLocalRearrangeSparkFactory;
import org.apache.pig.backend.hadoop.executionengine.spark.operator.POStreamSpark;
import org.apache.pig.backend.hadoop.executionengine.tez.plan.operator.POLocalRearrangeTez;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.builtin.FindQuantiles;
import org.apache.pig.impl.builtin.GetMemNumRows;
import org.apache.pig.impl.io.FileSpec;
import org.apache.pig.impl.plan.DepthFirstWalker;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.Operator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.OperatorPlan;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.plan.VisitorException;
import org.apache.pig.impl.util.Pair;

/**
 * The compiler that compiles a given physical physicalPlan into a DAG of Spark
 * operators
 */
public class SparkCompiler extends PhyPlanVisitor {
    private static final Log LOG = LogFactory.getLog(SparkCompiler.class);
	private PigContext pigContext;

	// The physicalPlan that is being compiled
	private PhysicalPlan physicalPlan;

	// The physicalPlan of Spark Operators
	private SparkOperPlan sparkPlan;

	private SparkOperator curSparkOp;

	private String scope;

	private SparkOperator[] compiledInputs = null;

	private Map<OperatorKey, SparkOperator> splitsSeen;

	private NodeIdGenerator nig;

	private Map<PhysicalOperator, SparkOperator> phyToSparkOpMap;

	private UDFFinder udfFinder;

    public static final String USER_COMPARATOR_MARKER = "user.comparator.func:";
    private POLocalRearrangeSparkFactory localRearrangeFactory;

	public SparkCompiler(PhysicalPlan physicalPlan, PigContext pigContext) {
		super(physicalPlan,
				new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(
						physicalPlan));
		this.physicalPlan = physicalPlan;
		this.pigContext = pigContext;
		this.sparkPlan = new SparkOperPlan();
		this.phyToSparkOpMap = new HashMap<PhysicalOperator, SparkOperator>();
		this.udfFinder = new UDFFinder();
		this.nig = NodeIdGenerator.getGenerator();
		this.splitsSeen = new HashMap<OperatorKey, SparkOperator>();
        this.localRearrangeFactory = new POLocalRearrangeSparkFactory(scope, nig);

	}

	public void compile() throws IOException, PlanException, VisitorException {
		List<PhysicalOperator> roots = physicalPlan.getRoots();
		if ((roots == null) || (roots.size() <= 0)) {
			int errCode = 2053;
			String msg = "Internal error. Did not find roots in the physical physicalPlan.";
			throw new SparkCompilerException(msg, errCode, PigException.BUG);
		}
		scope = roots.get(0).getOperatorKey().getScope();
		List<PhysicalOperator> leaves = physicalPlan.getLeaves();

		if (!pigContext.inIllustrator)
			for (PhysicalOperator op : leaves) {
				if (!(op instanceof POStore)) {
					int errCode = 2025;
					String msg = "Expected leaf of reduce physicalPlan to "
							+ "always be POStore. Found "
							+ op.getClass().getSimpleName();
					throw new SparkCompilerException(msg, errCode,
							PigException.BUG);
				}
			}

		// get all stores and nativeSpark operators, sort them in order(operator
		// id)
		// and compile their plans
		List<POStore> stores = PlanHelper.getPhysicalOperators(physicalPlan,
				POStore.class);
		List<PONative> nativeSparks = PlanHelper.getPhysicalOperators(
				physicalPlan, PONative.class);
		List<PhysicalOperator> ops;
		if (!pigContext.inIllustrator) {
			ops = new ArrayList<PhysicalOperator>(stores.size()
					+ nativeSparks.size());
			ops.addAll(stores);
		} else {
			ops = new ArrayList<PhysicalOperator>(leaves.size()
					+ nativeSparks.size());
			ops.addAll(leaves);
		}
		ops.addAll(nativeSparks);
		Collections.sort(ops);

		for (PhysicalOperator op : ops) {
			compile(op);
		}
	}

	/**
	 * Compiles the physicalPlan below op into a Spark Operator and stores it in
	 * curSparkOp.
	 * 
	 * @param op
	 * @throws IOException
	 * @throws PlanException
	 * @throws VisitorException
	 */
	private void compile(PhysicalOperator op) throws IOException,
			PlanException, VisitorException {
		SparkOperator[] prevCompInp = compiledInputs;

		List<PhysicalOperator> predecessors = physicalPlan.getPredecessors(op);
		if (op instanceof PONative) {
			// the predecessor (store) has already been processed
			// don't process it again
		} else if (predecessors != null && predecessors.size() > 0) {
			// When processing an entire script (multiquery), we can
			// get into a situation where a load has
			// predecessors. This means that it depends on some store
			// earlier in the physicalPlan. We need to take that dependency
			// and connect the respective Spark operators, while at the
			// same time removing the connection between the Physical
			// operators. That way the jobs will run in the right
			// order.
			if (op instanceof POLoad) {

				if (predecessors.size() != 1) {
					int errCode = 2125;
					String msg = "Expected at most one predecessor of load. Got "
							+ predecessors.size();
					throw new PlanException(msg, errCode, PigException.BUG);
				}

				PhysicalOperator p = predecessors.get(0);
				SparkOperator oper = null;
				if (p instanceof POStore || p instanceof PONative) {
					oper = phyToSparkOpMap.get(p);
				} else {
					int errCode = 2126;
					String msg = "Predecessor of load should be a store or spark operator. Got "
							+ p.getClass();
					throw new PlanException(msg, errCode, PigException.BUG);
				}

				// Need new operator
				curSparkOp = getSparkOp();
				curSparkOp.add(op);
				sparkPlan.add(curSparkOp);
				physicalPlan.disconnect(op, p);
				sparkPlan.connect(oper, curSparkOp);
				phyToSparkOpMap.put(op, curSparkOp);
				return;
			}

			Collections.sort(predecessors);
			compiledInputs = new SparkOperator[predecessors.size()];
			int i = -1;
			for (PhysicalOperator pred : predecessors) {
				if (pred instanceof POSplit
						&& splitsSeen.containsKey(pred.getOperatorKey())) {
					compiledInputs[++i] = startNew(
							((POSplit) pred).getSplitStore(),
							splitsSeen.get(pred.getOperatorKey()));
					continue;
				}
				compile(pred);
				compiledInputs[++i] = curSparkOp;
			}
		} else {
			// No predecessors. Mostly a load. But this is where
			// we start. We create a new sparkOp and add its first
			// operator op. Also this should be added to the sparkPlan.
			curSparkOp = getSparkOp();
			curSparkOp.add(op);
			if (op != null && op instanceof POLoad) {
				if (((POLoad) op).getLFile() != null
						&& ((POLoad) op).getLFile().getFuncSpec() != null)
					curSparkOp.UDFs.add(((POLoad) op).getLFile().getFuncSpec()
							.toString());
			}
			sparkPlan.add(curSparkOp);
			phyToSparkOpMap.put(op, curSparkOp);
			return;
		}
		op.visit(this);
		compiledInputs = prevCompInp;
	}

	private SparkOperator getSparkOp() {
		return new SparkOperator(OperatorKey.genOpKey(scope));
	}

	public SparkOperPlan getSparkPlan() {
		return sparkPlan;
	}

	public void connectSoftLink() throws PlanException, IOException {
		for (PhysicalOperator op : physicalPlan) {
			if (physicalPlan.getSoftLinkPredecessors(op) != null) {
				for (PhysicalOperator pred : physicalPlan
						.getSoftLinkPredecessors(op)) {
					SparkOperator from = phyToSparkOpMap.get(pred);
					SparkOperator to = phyToSparkOpMap.get(op);
					if (from == to)
						continue;
					if (sparkPlan.getPredecessors(to) == null
							|| !sparkPlan.getPredecessors(to).contains(from)) {
						sparkPlan.connect(from, to);
					}
				}
			}
		}
	}

	private SparkOperator startNew(FileSpec fSpec, SparkOperator old)
			throws PlanException {
		POLoad ld = getLoad();
		ld.setLFile(fSpec);
		SparkOperator ret = getSparkOp();
		ret.add(ld);
		sparkPlan.add(ret);
		sparkPlan.connect(old, ret);
		return ret;
	}

	private POLoad getLoad() {
		POLoad ld = new POLoad(new OperatorKey(scope, nig.getNextNodeId(scope)));
		ld.setPc(pigContext);
		ld.setIsTmpLoad(true);
		return ld;
	}

	@Override
	public void visitSplit(POSplit op) throws VisitorException {
		try {
			FileSpec fSpec = op.getSplitStore();
			SparkOperator sparkOp = endSingleInputPlanWithStr(fSpec);
			sparkOp.setSplitter(true);
			splitsSeen.put(op.getOperatorKey(), sparkOp);
			curSparkOp = startNew(fSpec, sparkOp);
			phyToSparkOpMap.put(op, curSparkOp);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	public void visitDistinct(PODistinct op) throws VisitorException {
		try {
			nonBlocking(op);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	private SparkOperator endSingleInputPlanWithStr(FileSpec fSpec)
			throws PlanException {
		if (compiledInputs.length > 1) {
			int errCode = 2023;
			String msg = "Received a multi input physicalPlan when expecting only a single input one.";
			throw new PlanException(msg, errCode, PigException.BUG);
		}
		SparkOperator sparkOp = compiledInputs[0]; // Load
		POStore str = getStore();
		str.setSFile(fSpec);
		sparkOp.physicalPlan.addAsLeaf(str);
		return sparkOp;
	}

	private POStore getStore() {
		POStore st = new POStore(new OperatorKey(scope,
				nig.getNextNodeId(scope)));
		// mark store as tmp store. These could be removed by the
		// optimizer, because it wasn't the user requesting it.
		st.setIsTmpStore(true);
		return st;
	}

	@Override
	public void visitLoad(POLoad op) throws VisitorException {
		try {
			nonBlocking(op);
			phyToSparkOpMap.put(op, curSparkOp);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void visitNative(PONative op) throws VisitorException {
		try {
			SparkOperator nativesparkOpper = getNativeSparkOp(
					op.getNativeMRjar(), op.getParams());
			sparkPlan.add(nativesparkOpper);
			sparkPlan.connect(curSparkOp, nativesparkOpper);
			phyToSparkOpMap.put(op, nativesparkOpper);
			curSparkOp = nativesparkOpper;
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	private NativeSparkOperator getNativeSparkOp(String sparkJar,
			String[] parameters) {
		return new NativeSparkOperator(new OperatorKey(scope,
				nig.getNextNodeId(scope)), sparkJar, parameters);
	}

	@Override
	public void visitStore(POStore op) throws VisitorException {
		try {
			nonBlocking(op);
			phyToSparkOpMap.put(op, curSparkOp);
			if (op.getSFile() != null && op.getSFile().getFuncSpec() != null)
				curSparkOp.UDFs.add(op.getSFile().getFuncSpec().toString());
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void visitFilter(POFilter op) throws VisitorException {
		try {
			nonBlocking(op);
			processUDFs(op.getPlan());
			phyToSparkOpMap.put(op, curSparkOp);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void visitCross(POCross op) throws VisitorException {
		try {
			nonBlocking(op);
			phyToSparkOpMap.put(op, curSparkOp);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void visitStream(POStream op) throws VisitorException {
		try {
			POStreamSpark poStreamSpark = new POStreamSpark(op);
			nonBlocking(poStreamSpark);
			phyToSparkOpMap.put(op, curSparkOp);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
    public void visitSort(POSort op) throws VisitorException {
        try {
//            SparkOperator prevOper = endSingleInputPlan();
//            SparkOperator sortOper = getSortJobs(op);
//            sparkPlan.connect(prevOper, sortOper);
//            curSparkOp = sortOper;

            nonBlocking(op);
            POSort sort = op;
            long limit = sort.getLimit();
            if (limit!=-1) {
                POLimit pLimit2 = new POLimit(new OperatorKey(scope,nig.getNextNodeId(scope)));
                pLimit2.setLimit(limit);
                curSparkOp.physicalPlan.addAsLeaf(pLimit2);
            }
            phyToSparkOpMap.put(op, curSparkOp);
        } catch (Exception e) {
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

    public void visitSort2(POSort op) throws VisitorException {
        try{
            Pair<POProject, Byte>[] fields = getSortCols(op.getSortPlans());
            byte keyType = DataType.UNKNOWN;

            try {
                FindKeyTypeVisitor fktv =
                        new FindKeyTypeVisitor(op.getSortPlans().get(0));
                fktv.visit();
                keyType = fktv.keyType;
            } catch (VisitorException ve) {
                int errCode = 2035;
                String msg = "Internal error. Could not compute key type of sort operator.";
                throw new PlanException(msg, errCode, PigException.BUG, ve);
            }
            POLocalRearrangeSpark lr = new POLocalRearrangeSpark(OperatorKey.genOpKey(scope));
            SparkOperator prevOper = endSingleInputWithStore2(op, lr, keyType, fields);
            SparkOperator sortOper = getSortJobs2(op,keyType,fields);
            lr.setOutputKey(sortOper.getOperatorKey().toString());
            sparkPlan.connect(prevOper,sortOper);
            curSparkOp = sortOper;
            phyToSparkOpMap.put(op, curSparkOp);
        } catch (Exception e) {
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
        }
    }

	public void visitSort1(POSort op) throws VisitorException {
        try{
            Pair<POProject, Byte>[] fields = getSortCols(op.getSortPlans());
            byte keyType = DataType.UNKNOWN;

            try {
                FindKeyTypeVisitor fktv =
                        new FindKeyTypeVisitor(op.getSortPlans().get(0));
                fktv.visit();
                keyType = fktv.keyType;
            } catch (VisitorException ve) {
                int errCode = 2035;
                String msg = "Internal error. Could not compute key type of sort operator.";
                throw new PlanException(msg, errCode, PigException.BUG, ve);
            }

            POLocalRearrangeSpark lr = new POLocalRearrangeSpark(OperatorKey.genOpKey(scope));
            POLocalRearrangeSpark lrSample = localRearrangeFactory.create(POLocalRearrangeSparkFactory.LocalRearrangeType.NULL);

            SparkOperator prevOper = endSingleInputWithStoreAndSample(op, lr, lrSample, keyType, fields);
            prevOper.markSampler();

            int rp = op.getRequestedParallelism();
            if (rp == -1) {
                rp = pigContext.defaultParallel;
            }

            Pair<SparkOperator, Integer> quantJobParallelismPair = getOrderbySamplingAggregationJob(op, rp);
            SparkOperator[] sortOpers = getSortJobs1(prevOper, lr, op, keyType, fields);

            sparkPlan.connect(prevOper,sortOpers[0]);
            sortOpers[0].setUseMRMapSettings(prevOper.isUseMRMapSettings());

            sortOpers[0].requestedParallelism = prevOper.requestedParallelism;
            if (rp==-1) {
                quantJobParallelismPair.first.setNeedEstimatedQuantile(true);
            }
            sortOpers[1].requestedParallelism = quantJobParallelismPair
                    .second;

            sparkPlan.connect(prevOper,quantJobParallelismPair.first);
            lr.setOutputKey(sortOpers[0].getOperatorKey().toString());
            lrSample.setOutputKey(quantJobParallelismPair.first.getOperatorKey().toString());

            sparkPlan.connect(sortOpers[0],sortOpers[1]);

            curSparkOp = sortOpers[1];

            //quantJobParallelismPair.first.setSortOperator(sortOpers[1]);

            // If Order by followed by Limit and parallelism of order by is not 1
            // add a new vertex for Limit with parallelism 1.
            // Equivalent of LimitAdjuster.java in MR
            if (op.isLimited() && rp != 1) {

                SparkOperator limitOper = getSparkOp();
                sparkPlan.add(limitOper);
                curSparkOp = limitOper;

                // Explicitly set the parallelism for the new vertex to 1.
                limitOper.requestedParallelism = 1;
                limitOper.markLimitAfterSort();

                sparkPlan.connect(sortOpers[1],limitOper);
                POLimit limit = new POLimit(OperatorKey.genOpKey(scope));
                limit.setLimit(op.getLimit());
                curSparkOp.physicalPlan.addAsLeaf(limit);
            }

            phyToSparkOpMap.put(op, curSparkOp);
        }catch(Exception e){
            int errCode = 2034;
            String msg = "Error compiling operator " + op.getClass().getSimpleName();
            throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
        }
	}

	@Override
	public void visitLimit(POLimit op) throws VisitorException {
		try {
			nonBlocking(op);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void visitLocalRearrange(POLocalRearrange op)
			throws VisitorException {
		try {
            POLocalRearrangeSpark lrSpark = new POLocalRearrangeSpark(op);
			nonBlocking(lrSpark);
			List<PhysicalPlan> plans = op.getPlans();
			if (plans != null)
				for (PhysicalPlan ep : plans)
					processUDFs(ep);
			phyToSparkOpMap.put(op, curSparkOp);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void visitCollectedGroup(POCollectedGroup op)
			throws VisitorException {
		List<PhysicalOperator> roots = curSparkOp.physicalPlan.getRoots();
		if (roots.size() != 1) {
			int errCode = 2171;
			String errMsg = "Expected one but found more then one root physical operator in physical physicalPlan.";
			throw new SparkCompilerException(errMsg, errCode, PigException.BUG);
		}

		PhysicalOperator phyOp = roots.get(0);
		if (!(phyOp instanceof POLoad)) {
			int errCode = 2172;
			String errMsg = "Expected physical operator at root to be POLoad. Found : "
					+ phyOp.getClass().getCanonicalName();
			throw new SparkCompilerException(errMsg, errCode, PigException.BUG);
		}

		LoadFunc loadFunc = ((POLoad) phyOp).getLoadFunc();
		try {
			if (!(CollectableLoadFunc.class.isAssignableFrom(loadFunc
					.getClass()))) {
				int errCode = 2249;
				throw new SparkCompilerException(
						"While using 'collected' on group; data must be loaded via loader implementing CollectableLoadFunc.",
						errCode);
			}
			((CollectableLoadFunc) loadFunc).ensureAllKeyInstancesInSameSplit();
		} catch (SparkCompilerException e) {
			throw (e);
		} catch (IOException e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}

		try {
			nonBlocking(op);
			phyToSparkOpMap.put(op, curSparkOp);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void visitPOForEach(POForEach op) throws VisitorException {
		try {
			nonBlocking(op);
			List<PhysicalPlan> plans = op.getInputPlans();
			if (plans != null) {
				for (PhysicalPlan ep : plans) {
					processUDFs(ep);
				}
			}
			phyToSparkOpMap.put(op, curSparkOp);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
    public void visitGlobalRearrange(POGlobalRearrange op)
			throws VisitorException {
		try {
			blocking(op);
			curSparkOp.customPartitioner = op.getCustomPartitioner();
			phyToSparkOpMap.put(op, curSparkOp);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void visitPackage(POPackage op) throws VisitorException {
		try {
			nonBlocking(op);
			phyToSparkOpMap.put(op, curSparkOp);
			if (op.getPkgr().getPackageType() == Packager.PackageType.JOIN) {
				curSparkOp.markRegularJoin();
			} else if (op.getPkgr().getPackageType() == Packager.PackageType.GROUP) {
				if (op.getNumInps() == 1) {
					curSparkOp.markGroupBy();
				} else if (op.getNumInps() > 1) {
					curSparkOp.markCogroup();
				}
			}

		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void visitUnion(POUnion op) throws VisitorException {
		try {
			nonBlocking(op);
			phyToSparkOpMap.put(op, curSparkOp);
		} catch (Exception e) {
			int errCode = 2034;
			String msg = "Error compiling operator "
					+ op.getClass().getSimpleName();
			throw new SparkCompilerException(msg, errCode, PigException.BUG, e);
		}
	}

	@Override
	public void visitSkewedJoin(POSkewedJoin op) throws VisitorException {
		// TODO
	}

	@Override
	public void visitFRJoin(POFRJoin op) throws VisitorException {
		// TODO
	}

	@Override
	public void visitMergeJoin(POMergeJoin joinOp) throws VisitorException {
		// TODO
	}

	private void processUDFs(PhysicalPlan plan) throws VisitorException {
		if (plan != null) {
			// Process Scalars (UDF with referencedOperators)
			ScalarPhyFinder scalarPhyFinder = new ScalarPhyFinder(plan);
			scalarPhyFinder.visit();
			curSparkOp.scalars.addAll(scalarPhyFinder.getScalars());

			// Process UDFs
			udfFinder.setPlan(plan);
			udfFinder.visit();
			curSparkOp.UDFs.addAll(udfFinder.getUDFs());
		}
	}

	private void nonBlocking(PhysicalOperator op) throws PlanException,
			IOException {
		SparkOperator sparkOp = null;
		if (compiledInputs.length == 1) {
			sparkOp = compiledInputs[0];
		} else {
			sparkOp = merge(compiledInputs);
		}
		sparkOp.physicalPlan.addAsLeaf(op);
		curSparkOp = sparkOp;
	}

	private void blocking(PhysicalOperator op) throws PlanException,
			IOException {
		SparkOperator newSparkOp = getSparkOp();
		sparkPlan.add(newSparkOp);
		for (SparkOperator sparkOp : compiledInputs) {
            connect(sparkPlan, sparkOp, newSparkOp);
		}

		newSparkOp.physicalPlan.addAsLeaf(op);
		curSparkOp = newSparkOp;
	}

    private void connect(SparkOperPlan sparkPlan, SparkOperator from , SparkOperator to) throws PlanException {
        sparkPlan.connect(from, to);
        if (!from.physicalPlan.isEmpty()) {
            PhysicalOperator leaf = from.physicalPlan.getLeaves().get(0);
            if (leaf instanceof POLocalRearrangeSpark) {
                POLocalRearrangeSpark lr = (POLocalRearrangeSpark) leaf;
                lr.setOutputKey(to.getOperatorKey().toString());
            }
        }
    }

	private SparkOperator merge(SparkOperator[] compiledInputs)
			throws PlanException {
		SparkOperator ret = getSparkOp();
		sparkPlan.add(ret);

		Set<SparkOperator> toBeConnected = new HashSet<SparkOperator>();
		List<SparkOperator> toBeRemoved = new ArrayList<SparkOperator>();

		List<PhysicalPlan> toBeMerged = new ArrayList<PhysicalPlan>();

		for (SparkOperator sparkOp : compiledInputs) {
			toBeRemoved.add(sparkOp);
			toBeMerged.add(sparkOp.physicalPlan);
			List<SparkOperator> predecessors = sparkPlan
					.getPredecessors(sparkOp);
			if (predecessors != null) {
				for (SparkOperator predecessorSparkOp : predecessors) {
					toBeConnected.add(predecessorSparkOp);
				}
			}
		}
		merge(ret.physicalPlan, toBeMerged);

		Iterator<SparkOperator> it = toBeConnected.iterator();
		while (it.hasNext())
			sparkPlan.connect(it.next(), ret);
		for (SparkOperator removeSparkOp : toBeRemoved) {
			if (removeSparkOp.requestedParallelism > ret.requestedParallelism)
				ret.requestedParallelism = removeSparkOp.requestedParallelism;
			for (String udf : removeSparkOp.UDFs) {
				if (!ret.UDFs.contains(udf))
					ret.UDFs.add(udf);
			}
			// We also need to change scalar marking
			for (PhysicalOperator physOp : removeSparkOp.scalars) {
				if (!ret.scalars.contains(physOp)) {
					ret.scalars.add(physOp);
				}
			}
			Set<PhysicalOperator> opsToChange = new HashSet<PhysicalOperator>();
			for (Map.Entry<PhysicalOperator, SparkOperator> entry : phyToSparkOpMap
					.entrySet()) {
				if (entry.getValue() == removeSparkOp) {
					opsToChange.add(entry.getKey());
				}
			}
			for (PhysicalOperator op : opsToChange) {
				phyToSparkOpMap.put(op, ret);
			}

			sparkPlan.remove(removeSparkOp);
		}
		return ret;
	}

	/**
	 * The merge of a list of plans into a single physicalPlan
	 * 
	 * @param <O>
	 * @param <E>
	 * @param finPlan
	 *            - Final Plan into which the list of plans is merged
	 * @param plans
	 *            - list of plans to be merged
	 * @throws PlanException
	 */
	private <O extends Operator<?>, E extends OperatorPlan<O>> void merge(
			E finPlan, List<E> plans) throws PlanException {
		for (E e : plans) {
			finPlan.merge(e);
		}
	}

    public static Pair<POProject, Byte>[] getSortCols(List<PhysicalPlan>
                                                              plans) throws PlanException {
        if(plans!=null){
            @SuppressWarnings("unchecked")
            Pair<POProject,Byte>[] ret = new Pair[plans.size()];
            int i=-1;
            for (PhysicalPlan plan : plans) {
                PhysicalOperator op = plan.getLeaves().get(0);
                POProject proj;
                if (op instanceof POProject) {
                    if (((POProject)op).isStar()) return null;
                    proj = (POProject)op;
                } else {
                    proj = null;
                }
                byte type = op.getResultType();
                ret[++i] = new Pair<POProject, Byte>(proj, type);
            }
            return ret;
        }
        int errCode = 2026;
        String msg = "No expression physicalPlan found in POSort.";
        throw new PlanException(msg, errCode, PigException.BUG);
    }

    private SparkOperator endSingleInputPlan(){
        SparkOperator oper = compiledInputs[0];
        return oper;
    }

    private SparkOperator endSingleInputWithStore2( POSort sort,
                                                    POLocalRearrangeSpark lr,  byte keyType,Pair<POProject, Byte>[] fields)
            throws PlanException {
        SparkOperator oper = compiledInputs[0];

        List<PhysicalPlan> eps = new ArrayList<PhysicalPlan>();
        if (fields == null) {
            // This is project *
            PhysicalPlan ep = new PhysicalPlan();
            POProject prj = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
            prj.setStar(true);
            prj.setOverloaded(false);
            prj.setResultType(DataType.TUPLE);
            ep.add(prj);
            eps.add(ep);
        } else {
            // Attach the sort plans to the local rearrange to get the
            // projection.
            eps.addAll(sort.getSortPlans());
        }

        try {
            lr.setIndex(0);
        } catch (ExecException e) {
            int errCode = 2058;
            String msg = "Unable to set index on newly created " +
                    "POLocalRearrangeSpark.";
            throw new PlanException(msg, errCode, PigException.BUG, e);
        }
        lr.setKeyType((fields == null || fields.length>1) ? DataType.TUPLE : keyType);
        lr.setPlans(eps);
        lr.setResultType(DataType.TUPLE);
        lr.addOriginalLocation(sort.getAlias(), sort.getOriginalLocations());

        lr.setOutputKey(curSparkOp.getOperatorKey().toString());
        oper.physicalPlan.addAsLeaf(lr);
        return oper;
    }


    private SparkOperator endSingleInputWithStoreAndSample(
            POSort sort,
            POLocalRearrangeSpark lr,
            POLocalRearrangeSpark lrSample,
            byte keyType,
            Pair<POProject, Byte>[] fields) throws PlanException {
        if(compiledInputs.length>1) {
            int errCode = 2023;
            String msg = "Received a multi input plan when expecting only a single input one.";
            throw new PlanException(msg, errCode, PigException.BUG);
        }
        SparkOperator oper = compiledInputs[0];

            List<PhysicalPlan> eps = new ArrayList<PhysicalPlan>();
            if (fields == null) {
                // This is project *
                PhysicalPlan ep = new PhysicalPlan();
                POProject prj = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
                prj.setStar(true);
                prj.setOverloaded(false);
                prj.setResultType(DataType.TUPLE);
                ep.add(prj);
                eps.add(ep);
            } else {
                // Attach the sort plans to the local rearrange to get the
                // projection.
                eps.addAll(sort.getSortPlans());
            }

            try {
                lr.setIndex(0);
            } catch (ExecException e) {
                int errCode = 2058;
                String msg = "Unable to set index on newly created " +
                        "POLocalRearrangeSpark.";
                throw new PlanException(msg, errCode, PigException.BUG, e);
            }
            lr.setKeyType((fields == null || fields.length>1) ? DataType.TUPLE : keyType);
            lr.setPlans(eps);
            lr.setResultType(DataType.TUPLE);
            lr.addOriginalLocation(sort.getAlias(), sort.getOriginalLocations());

            lr.setOutputKey(curSparkOp.getOperatorKey().toString());
            oper.physicalPlan.addAsLeaf(lr);

            List<Boolean> flat1 = new ArrayList<Boolean>();
            List<PhysicalPlan> eps1 = new ArrayList<PhysicalPlan>();

            Pair<POProject, Byte>[] sortProjs = null;
            try{
                sortProjs = getSortCols(sort.getSortPlans());
            }catch(Exception e) {
                throw new RuntimeException(e);
            }
            // Set up the projections of the key columns
            if (sortProjs == null) {
                PhysicalPlan ep = new PhysicalPlan();
                POProject prj = new POProject(new OperatorKey(scope,
                        nig.getNextNodeId(scope)));
                prj.setStar(true);
                prj.setOverloaded(false);
                prj.setResultType(DataType.TUPLE);
                ep.add(prj);
                eps1.add(ep);
                flat1.add(false);
            } else {
                for (Pair<POProject, Byte> sortProj : sortProjs) {
                    // Check for proj being null, null is used by getSortCols for a non POProject
                    // operator. Since Order by does not allow expression operators,
                    //it should never be set to null
                    if(sortProj == null){
                        int errCode = 2174;
                        String msg = "Internal exception. Could not create a sampler job";
                        throw new PlanException(msg, errCode, PigException.BUG);
                    }
                    PhysicalPlan ep = new PhysicalPlan();
                    POProject prj;
                    try {
                        prj = sortProj.first.clone();
                    } catch (CloneNotSupportedException e) {
                        //should not get here
                        throw new AssertionError(
                                "Error cloning project caught exception" + e
                        );
                    }
                    ep.add(prj);
                    eps1.add(ep);
                    flat1.add(false);
                }
            }

            String numSamples = pigContext.getProperties().getProperty(PigConfiguration.PIG_RANDOM_SAMPLER_SAMPLE_SIZE, "100");
            POReservoirSample poSample = new POReservoirSample(new OperatorKey(scope,nig.getNextNodeId(scope)),
                    -1, null, Integer.parseInt(numSamples));
            oper.physicalPlan.addAsLeaf(poSample);

            List<PhysicalPlan> sortPlans = sort.getSortPlans();
            // Set up transform plan to get keys and memory size of input
            // tuples. It first adds all the plans to get key columns.
            List<PhysicalPlan> transformPlans = new ArrayList<PhysicalPlan>();
            transformPlans.addAll(sortPlans);

            // Then it adds a column for memory size
            POProject prjStar = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
            prjStar.setResultType(DataType.TUPLE);
            prjStar.setStar(true);

            List<PhysicalOperator> ufInps = new ArrayList<PhysicalOperator>();
            ufInps.add(prjStar);

            PhysicalPlan ep = new PhysicalPlan();
            POUserFunc uf = new POUserFunc(new OperatorKey(scope,nig.getNextNodeId(scope)),
                    -1, ufInps, new FuncSpec(GetMemNumRows.class.getName(), (String[])null));
            uf.setResultType(DataType.TUPLE);
            ep.add(uf);
            ep.add(prjStar);
            ep.connect(prjStar, uf);

            transformPlans.add(ep);

            flat1 = new ArrayList<Boolean>();
            eps1 = new ArrayList<PhysicalPlan>();

            for (int i=0; i<transformPlans.size(); i++) {
                eps1.add(transformPlans.get(i));
                if (i<sortPlans.size()) {
                    flat1.add(false);
                } else {
                    flat1.add(true);
                }
            }

            // This foreach will pick the sort key columns from the POPoissonSample output
            POForEach nfe1 = new POForEach(new OperatorKey(scope,nig.getNextNodeId(scope)),
                    -1, eps1, flat1);
            oper.physicalPlan.addAsLeaf(nfe1);
            lrSample.setOutputKey(curSparkOp.getOperatorKey().toString());
            oper.physicalPlan.addAsLeaf(lrSample);
        return oper;
    }


    private Pair<SparkOperator,Integer> getOrderbySamplingAggregationJob(
            POSort inpSort,
            int rp) throws PlanException, VisitorException, ExecException {

        POSort sort = new POSort(inpSort.getOperatorKey(), inpSort
                .getRequestedParallelism(), null, inpSort.getSortPlans(),
                inpSort.getMAscCols(), inpSort.getMSortFunc());
        sort.addOriginalLocation(inpSort.getAlias(), inpSort.getOriginalLocations());

        // Turn the asc/desc array into an array of strings so that we can pass it
        // to the FindQuantiles function.
        List<Boolean> ascCols = inpSort.getMAscCols();
        String[] ascs = new String[ascCols.size()];
        for (int i = 0; i < ascCols.size(); i++) ascs[i] = ascCols.get(i).toString();
        // check if user defined comparator is used in the sort, if so
        // prepend the name of the comparator as the first fields in the
        // constructor args array to the FindQuantiles udf
        String[] ctorArgs = ascs;
        if(sort.isUDFComparatorUsed) {
            String userComparatorFuncSpec = sort.getMSortFunc().getFuncSpec().toString();
            ctorArgs = new String[ascs.length + 1];
            ctorArgs[0] = USER_COMPARATOR_MARKER + userComparatorFuncSpec;
            for(int j = 0; j < ascs.length; j++) {
                ctorArgs[j+1] = ascs[j];
            }
        }

        return getSamplingAggregationJob(sort, rp, null, FindQuantiles.class.getName(), ctorArgs);
    }

    /**
     * Create a sampling job to collect statistics by sampling input data. The
     * sequence of operations is as following:
     * <li>Add an extra field &quot;all&quot; into the tuple </li>
     * <li>Package all tuples into one bag </li>
     * <li>Add constant field for number of reducers. </li>
     * <li>Sorting the bag </li>
     * <li>Invoke UDF with the number of reducers and the sorted bag.</li>
     * <li>Data generated by UDF is transferred via a broadcast edge.</li>
     *
     * @param sort  the POSort operator used to sort the bag
     * @param rp  configured parallemism
     * @param sortKeyPlans  PhysicalPlans to be set into POSort operator to get sorting keys
     * @param udfClassName  the class name of UDF
     * @param udfArgs   the arguments of UDF
     * @return pair<SparkOperator[],integer>
     * @throws PlanException
     * @throws VisitorException
     * @throws ExecException
     */
    private Pair<SparkOperator,Integer> getSamplingAggregationJob(POSort sort, int rp,
                                                                List<PhysicalPlan> sortKeyPlans, String udfClassName, String[] udfArgs)
            throws PlanException, VisitorException, ExecException {

        SparkOperator oper = getSparkOp();
        sparkPlan.add(oper);

        POGlobalRearrange glb = new POGlobalRearrange(OperatorKey.genOpKey(scope));
        oper.physicalPlan.add(glb);

        POPackage pkg = getPackage(1, DataType.BYTEARRAY);
        oper.physicalPlan.add(pkg);
        oper.physicalPlan.connect(glb, pkg);
        // Lets start building the plan which will have the sort
        // for the foreach
        PhysicalPlan fe2Plan = new PhysicalPlan();
        // Top level project which just projects the tuple which is coming
        // from the foreach after the package
        POProject topPrj = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
        topPrj.setColumn(1);
        topPrj.setResultType(DataType.BAG);
        topPrj.setOverloaded(true);
        fe2Plan.add(topPrj);

        // the projections which will form sort plans
        List<PhysicalPlan> nesSortPlanLst = new ArrayList<PhysicalPlan>();
        if (sortKeyPlans != null) {
            for(int i=0; i<sortKeyPlans.size(); i++) {
                nesSortPlanLst.add(sortKeyPlans.get(i));
            }
        }else{
            Pair<POProject, Byte>[] sortProjs = null;
            try{
                sortProjs = getSortCols(sort.getSortPlans());
            }catch(Exception e) {
                throw new RuntimeException(e);
            }
            // Set up the projections of the key columns
            if (sortProjs == null) {
                PhysicalPlan ep = new PhysicalPlan();
                POProject prj = new POProject(new OperatorKey(scope,
                        nig.getNextNodeId(scope)));
                prj.setStar(true);
                prj.setOverloaded(false);
                prj.setResultType(DataType.TUPLE);
                ep.add(prj);
                nesSortPlanLst.add(ep);
            } else {
                for (int i=0; i<sortProjs.length; i++) {
                    POProject prj =
                            new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));

                    prj.setResultType(sortProjs[i].second);
                    if(sortProjs[i].first != null && sortProjs[i].first.isProjectToEnd()){
                        if(i != sortProjs.length -1){
                            //project to end has to be the last sort column
                            throw new AssertionError("Project-range to end (x..)" +
                                    " is supported in order-by only as last sort column");
                        }
                        prj.setProjectToEnd(i);
                        break;
                    }
                    else{
                        prj.setColumn(i);
                    }
                    prj.setOverloaded(false);

                    PhysicalPlan ep = new PhysicalPlan();
                    ep.add(prj);
                    nesSortPlanLst.add(ep);
                }
            }
        }

        sort.setSortPlans(nesSortPlanLst);
        sort.setResultType(DataType.BAG);
        fe2Plan.add(sort);
        fe2Plan.connect(topPrj, sort);

        // The plan which will have a constant representing the
        // degree of parallelism for the final order by map-reduce job
        // this will either come from a "order by parallel x" in the script
        // or will be the default number of reducers for the cluster if
        // "parallel x" is not used in the script
        PhysicalPlan rpep = new PhysicalPlan();
        ConstantExpression rpce = new ConstantExpression(new OperatorKey(scope,nig.getNextNodeId(scope)));
        rpce.setRequestedParallelism(rp);

        // We temporarily set it to rp and will adjust it at runtime, because the final degree of parallelism
        // is unknown until we are ready to submit it. See PIG-2779.
        rpce.setValue(rp);

        rpce.setResultType(DataType.INTEGER);
        rpep.add(rpce);

        List<PhysicalPlan> genEps = new ArrayList<PhysicalPlan>();
        genEps.add(rpep);
        genEps.add(fe2Plan);

        List<Boolean> flattened2 = new ArrayList<Boolean>();
        flattened2.add(false);
        flattened2.add(false);

        POForEach nfe2 = new POForEach(new OperatorKey(scope,nig.getNextNodeId(scope)),-1, genEps, flattened2);
        oper.physicalPlan.add(nfe2);
        oper.physicalPlan.connect(pkg, nfe2);

        // Let's connect the output from the foreach containing
        // number of quantiles and the sorted bag of samples to
        // another foreach with the FindQuantiles udf. The input
        // to the FindQuantiles udf is a project(*) which takes the
        // foreach input and gives it to the udf
        PhysicalPlan ep4 = new PhysicalPlan();
        POProject prjStar4 = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
        prjStar4.setResultType(DataType.TUPLE);
        prjStar4.setStar(true);
        ep4.add(prjStar4);

        List<PhysicalOperator> ufInps = new ArrayList<PhysicalOperator>();
        ufInps.add(prjStar4);

        POUserFunc uf = new POUserFunc(new OperatorKey(scope,nig.getNextNodeId(scope)), -1, ufInps,
                new FuncSpec(udfClassName, udfArgs));
        ep4.add(uf);
        ep4.connect(prjStar4, uf);

        List<PhysicalPlan> ep4s = new ArrayList<PhysicalPlan>();
        ep4s.add(ep4);
        List<Boolean> flattened3 = new ArrayList<Boolean>();
        flattened3.add(false);
        POForEach nfe3 = new POForEach(new OperatorKey(scope,nig.getNextNodeId(scope)), -1, ep4s, flattened3);

        oper.physicalPlan.add(nfe3);
        oper.physicalPlan.connect(nfe2, nfe3);

        oper.requestedParallelism=1;
        oper.setDontEstimateParallelism(true);
        oper.markSampleAggregation();
        return new Pair<SparkOperator, Integer>(oper, rp);
    }

    private SparkOperator getSortJobs2(POSort sort,byte keyType,Pair<POProject, Byte>[] fields) throws PlanException{
        long limit = sort.getLimit();
        SparkOperator oper2 = getSparkOp();
        oper2.setGlobalSort();
        POGlobalRearrange glb = new POGlobalRearrange(OperatorKey.genOpKey
                (scope));
        oper2.physicalPlan.add(glb);

        POPackage pkg = new POPackage(OperatorKey.genOpKey(scope));
        pkg.setPkgr(new LitePackager());
        pkg.getPkgr().setKeyType((fields == null || fields.length > 1) ? DataType.TUPLE : keyType);
        pkg.setNumInps(1);
        oper2.physicalPlan.add(pkg);
        oper2.physicalPlan.connect(glb, pkg);

        PhysicalPlan ep = new PhysicalPlan();
        POProject prj = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
        prj.setColumn(1);
        prj.setOverloaded(false);
        prj.setResultType(DataType.BAG);
        ep.add(prj);
        List<PhysicalPlan> eps2 = new ArrayList<PhysicalPlan>();
        eps2.add(ep);
        List<Boolean> flattened = new ArrayList<Boolean>();
        flattened.add(true);
        POForEach nfe1 = new POForEach(new OperatorKey(scope,nig.getNextNodeId(scope)),-1,eps2,flattened);
        oper2.physicalPlan.add(nfe1);
        oper2.physicalPlan.connect(pkg, nfe1);
        if (limit!=-1) {
            oper2.physicalPlan.addAsLeaf(sort);
            POLimit pLimit2 = new POLimit(new OperatorKey(scope,nig.getNextNodeId(scope)));
            pLimit2.setLimit(limit);
            oper2.physicalPlan.addAsLeaf(pLimit2);
        }
        sparkPlan.add(oper2);
        return oper2;

    }


    private SparkOperator getSortJobs(POSort sort) throws PlanException{
        long limit = sort.getLimit();
        SparkOperator oper2 = getSparkOp();
        oper2.setGlobalSort();
        if (limit!=-1) {
            oper2.physicalPlan.addAsLeaf(sort);
            POLimit pLimit2 = new POLimit(new OperatorKey(scope,nig.getNextNodeId(scope)));
            pLimit2.setLimit(limit);
            oper2.physicalPlan.addAsLeaf(pLimit2);
        }
        sparkPlan.add(oper2);
        return oper2;
    }

    private SparkOperator[] getSortJobs1(
            SparkOperator inputOper,
            POLocalRearrangeSpark inputOperRearrange,
            POSort sort,
            byte keyType,
            Pair<POProject, Byte>[] fields) throws PlanException{
        SparkOperator[] opers = new SparkOperator[2];
        SparkOperator oper1 = getSparkOp();
        sparkPlan.add(oper1);
        opers[0] = oper1;

        POLocalRearrangeSpark identityInOutSpark = new POLocalRearrangeSpark
                (inputOperRearrange);
        oper1.physicalPlan.addAsLeaf(identityInOutSpark);
        oper1.markSampleBasedPartitioner();

        SparkOperator oper2 = getSparkOp();
        oper2.setGlobalSort();
        opers[1] = oper2;
        sparkPlan.add(oper2);

        long limit = sort.getLimit();

        boolean[] sortOrder;

        List<Boolean> sortOrderList = sort.getMAscCols();
        if(sortOrderList != null) {
            sortOrder = new boolean[sortOrderList.size()];
            for(int i = 0; i < sortOrderList.size(); ++i) {
                sortOrder[i] = sortOrderList.get(i);
            }
            oper2.setSortOrder(sortOrder);
        }

        identityInOutSpark.setOutputKey(oper2.getOperatorKey().toString());

        POGlobalRearrange glb = new POGlobalRearrange(OperatorKey.genOpKey
                (scope));
        oper2.physicalPlan.add(glb);

        POPackage pkg = new POPackage(OperatorKey.genOpKey(scope));
        pkg.setPkgr(new LitePackager());
        pkg.getPkgr().setKeyType((fields == null || fields.length > 1) ? DataType.TUPLE : keyType);
        pkg.setNumInps(1);
        oper2.physicalPlan.add(pkg);
        oper2.physicalPlan.connect(glb, pkg);

        PhysicalPlan ep = new PhysicalPlan();
        POProject prj = new POProject(new OperatorKey(scope,nig.getNextNodeId(scope)));
        prj.setColumn(1);
        prj.setOverloaded(false);
        prj.setResultType(DataType.BAG);
        ep.add(prj);
        List<PhysicalPlan> eps2 = new ArrayList<PhysicalPlan>();
        eps2.add(ep);
        List<Boolean> flattened = new ArrayList<Boolean>();
        flattened.add(true);
        POForEach nfe1 = new POForEach(new OperatorKey(scope,nig.getNextNodeId(scope)),-1,eps2,flattened);
        oper2.physicalPlan.add(nfe1);
        oper2.physicalPlan.connect(pkg, nfe1);
        if (limit!=-1) {
            POLimit pLimit2 = new POLimit(new OperatorKey(scope,nig.getNextNodeId(scope)));
            pLimit2.setLimit(limit);
            oper2.physicalPlan.addAsLeaf(pLimit2);
        }

        return opers;
    }

    private static class FindKeyTypeVisitor extends PhyPlanVisitor {

        byte keyType = DataType.UNKNOWN;

        FindKeyTypeVisitor(PhysicalPlan plan) {
            super(plan,
                    new DepthFirstWalker<PhysicalOperator, PhysicalPlan>(plan));
        }

        @Override
        public void visitProject(POProject p) throws VisitorException {
            keyType = p.getResultType();
        }
    }

    /**
     * Returns a POPackage with default packager. This method shouldn't be used
     * if special packager such as LitePackager and CombinerPackager is needed.
     */
    private POPackage getPackage(int numOfInputs, byte keyType) {
        // The default value of boolean is false
        boolean[] inner = new boolean[numOfInputs];
        POPackage pkg = new POPackage(OperatorKey.genOpKey(scope));
        pkg.getPkgr().setInner(inner);
        pkg.getPkgr().setKeyType(keyType);
        pkg.setNumInps(numOfInputs);
        return pkg;
    }

//    private void shipFiles(String shipFiles, String currentDirectoryPath)
//            throws IOException {
//        if (shipFiles != null) {
//            for (String file : shipFiles.split(",")) {
//                File shipFile = new File(file.trim());
//                if (shipFile.exists()) {
//                    LOG.info(String.format("shipFile:%s", shipFile));
//                    boolean isLocal = System.getenv("SPARK_MASTER") != null ? System
//                            .getenv("SPARK_MASTER").equalsIgnoreCase("LOCAL")
//                            : true;
//                    if (isLocal) {
//                        File localFile = new File(currentDirectoryPath + "/"
//                                + shipFile.getName());
//                        if (localFile.exists()) {
//                            LOG.info(String.format(
//                                    "ship file %s exists, ready to delete",
//                                    localFile.getAbsolutePath()));
//                            localFile.delete();
//                        } else {
//                            LOG.info(String.format("ship file %s  not exists,",
//                                    localFile.getAbsolutePath()));
//                        }
//                        Files.copy(shipFile.toPath(),
//                                Paths.get(localFile.getAbsolutePath()));
//                    } else {
//                        sparkContext.addFile(shipFile.toURI().toURL()
//                                .toExternalForm());
//                    }
//                }
//            }
//        }
//    }
}
