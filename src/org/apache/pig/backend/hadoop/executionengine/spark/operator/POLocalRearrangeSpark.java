package org.apache.pig.backend.hadoop.executionengine.spark.operator;

import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POLocalRearrange;
import org.apache.pig.impl.plan.NodeIdGenerator;
import org.apache.pig.impl.plan.OperatorKey;

/**

 */
public class POLocalRearrangeSpark extends POLocalRearrange {
    protected String outputKey;
    protected boolean connectedToPackage = true;
    public POLocalRearrangeSpark(OperatorKey k) {
        super(k);
    }

    public POLocalRearrangeSpark(OperatorKey k, int rp) {
        super(k, rp);
    }

    public POLocalRearrangeSpark(POLocalRearrange copy) {
        super(copy);
        if (copy instanceof POLocalRearrangeSpark) {
            POLocalRearrangeSpark copySpark = (POLocalRearrangeSpark) copy;
            this.connectedToPackage = copySpark.connectedToPackage;
            this.outputKey = copySpark.outputKey;
        }
    }


    public String getOutputKey() {
        return outputKey;
    }

    public void setOutputKey(String outputKey) {
        this.outputKey = outputKey;
    }

    public boolean isConnectedToPackage() {
        return connectedToPackage;
    }

    public void setConnectedToPackage(boolean connectedToPackage) {
        this.connectedToPackage = connectedToPackage;
    }

    @Override
    public POLocalRearrangeSpark clone() throws CloneNotSupportedException {
        POLocalRearrangeSpark clone = new POLocalRearrangeSpark(new OperatorKey(
                mKey.scope, NodeIdGenerator.getGenerator().getNextNodeId(
                mKey.scope)), requestedParallelism);
        deepCopyTo(clone);
        clone.connectedToPackage = connectedToPackage;
        clone.setOutputKey(outputKey);
        return clone;
    }
}
