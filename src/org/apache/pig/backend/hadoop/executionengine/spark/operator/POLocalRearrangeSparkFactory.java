package org.apache.pig.backend.hadoop.executionengine.spark.operator;

import java.util.List;

import com.google.common.collect.Lists;

import org.apache.pig.PigException;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ConstantExpression;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.ExpressionOperator;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.expressionOperators.POProject;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.plans.PhysicalPlan;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.backend.hadoop.executionengine.spark.plan.SparkCompiler;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;
import org.apache.pig.impl.plan.PlanException;
import org.apache.pig.impl.util.Pair;

public class POLocalRearrangeSparkFactory {
    public static enum LocalRearrangeType {
        STAR,
        NULL,
        NORMAL,
        WITHPLAN
    };

    private String scope;
    private NodeIdGenerator nig;

    public POLocalRearrangeSparkFactory(String scope, NodeIdGenerator nig) {
        this.scope = scope;
        this.nig = nig;
    }

    public POLocalRearrangeSpark create() throws PlanException {
        return create(0, LocalRearrangeType.STAR, null, DataType.UNKNOWN);
    }

    public POLocalRearrangeSpark create(LocalRearrangeType type) throws PlanException {
        return create(0, type, null, DataType.UNKNOWN);
    }

    public POLocalRearrangeSpark create(int index, LocalRearrangeType type) throws PlanException {
        return create(index, type, null, DataType.UNKNOWN);
    }

    public POLocalRearrangeSpark create(int index, LocalRearrangeType type, List<PhysicalPlan> plans,
                                      byte keyType) throws PlanException {
        ExpressionOperator keyExpression = null;

        if (type == LocalRearrangeType.STAR) {
            keyExpression = new POProject(new OperatorKey(scope, nig.getNextNodeId(scope)));
            keyExpression.setResultType(DataType.TUPLE);
            ((POProject)keyExpression).setStar(true);
        } else if (type == LocalRearrangeType.NULL) {
            keyExpression = new ConstantExpression(new OperatorKey(scope, nig.getNextNodeId(scope)));
            ((ConstantExpression)keyExpression).setValue(null);
            keyExpression.setResultType(DataType.BYTEARRAY);
        }

        PhysicalPlan addPlan = new PhysicalPlan();
        List<PhysicalPlan> addPlans = Lists.newArrayList();
        if (type == LocalRearrangeType.STAR || type == LocalRearrangeType.NULL) {
            addPlan.add(keyExpression);
            addPlans.add(addPlan);
        } else if (type == LocalRearrangeType.WITHPLAN) {
            addPlans.addAll(plans);
        }

        POLocalRearrangeSpark lr = new POLocalRearrangeSpark(new OperatorKey(scope, nig.getNextNodeId(scope)));
        try {
            lr.setIndex(index);
        } catch (ExecException e) {
            int errCode = 2058;
            String msg = "Unable to set index on the newly created POLocalRearrange.";
            throw new PlanException(msg, errCode, PigException.BUG, e);
        }
        if (type == LocalRearrangeType.STAR) {
            lr.setKeyType(DataType.TUPLE);
        } else if (type == LocalRearrangeType.NULL) {
            lr.setKeyType(DataType.BYTEARRAY);
        } else if (type == LocalRearrangeType.WITHPLAN) {
            Pair<POProject, Byte>[] fields = SparkCompiler.getSortCols(plans);
            lr.setKeyType((fields == null || fields.length>1) ? DataType.TUPLE : keyType);
        }
        lr.setResultType(DataType.TUPLE);
        lr.setPlans(addPlans);
        return lr;
    }

}
