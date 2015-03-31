package org.apache.pig.backend.hadoop.executionengine.spark.converter;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.mapred.JobConf;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigMapReduce;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.Result;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POReservoirSample;
import org.apache.pig.backend.hadoop.executionengine.spark.KryoSerializer;
import org.apache.pig.backend.hadoop.executionengine.spark.SparkUtil;
import org.apache.pig.backend.hadoop.executionengine.util.MapRedUtil;
import org.apache.pig.data.SchemaTupleBackend;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.util.ObjectSerializer;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;

/**

 */
public class ReservoirSampleConverter implements
        POConverter<Tuple, Tuple, POReservoirSample>, Serializable {
    private byte[] confBytes;

    public ReservoirSampleConverter(byte[] confBytes) {
        this.confBytes = confBytes;
    }

    @Override
    public RDD<Tuple> convert(List<RDD<Tuple>> predecessors, POReservoirSample physicalOperator) throws IOException {
        SparkUtil.assertPredecessorSize(predecessors, physicalOperator, 1);
        RDD<Tuple> rdd = predecessors.get(0);
        ReservoirSampleFunction reservoirSampleFunction = new ReservoirSampleFunction
                (physicalOperator, this.confBytes);
        return rdd.toJavaRDD().mapPartitions(reservoirSampleFunction, true).rdd();
    }

    private static class ReservoirSampleFunction implements
            FlatMapFunction<Iterator<Tuple>, Tuple>, Serializable {
        private POReservoirSample poReservoirSample;
        private byte[] confBytes;
        private transient JobConf jobConf;

        void initializeJobConf() {
            if (this.jobConf == null) {
                this.jobConf = KryoSerializer.deserializeJobConf(this.confBytes);
                PigMapReduce.sJobConfInternal.set(jobConf);
                try {
                    MapRedUtil.setupUDFContext(jobConf);
                    PigContext pc = (PigContext) ObjectSerializer.deserialize(jobConf.get("pig.pigContext"));
                    SchemaTupleBackend.initialize(jobConf, pc);

                } catch (IOException ioe) {
                    String msg = "Problem while configuring UDFContext from ForEachConverter.";
                    throw new RuntimeException(msg, ioe);
                }
            }
        }

        private ReservoirSampleFunction(POReservoirSample poReservoirSample, byte[]
                confBytes) {
            this.poReservoirSample = poReservoirSample;
            this.confBytes = confBytes;
        }
        public Iterable<Tuple> call(final Iterator<Tuple> input) {
            initializeJobConf();
            return new Iterable<Tuple>() {

                @Override
                public Iterator<Tuple> iterator() {
                    return new POOutputConsumerIterator(input) {

                        protected void attach(Tuple tuple) {
                            poReservoirSample.setInputs(null);
                            poReservoirSample.attachInput(tuple);
                        }

                        protected Result getNextResult() throws ExecException {
                            return poReservoirSample.getNextTuple();
                        }
                    };
                }
            };
        }
    }
}
