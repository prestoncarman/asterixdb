package edu.uci.ics.asterix.jobgen;

import java.nio.ByteBuffer;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.uci.ics.asterix.metadata.statistics.BaseStatistics;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.context.RuntimeContext;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.operators.base.AbstractOneInputOneOutputOneFramePushRuntime;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.operators.base.AbstractOneInputOneOutputPushRuntime;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.operators.base.AbstractOneInputOneOutputRuntimeFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.messages.IMessage;
import edu.uci.ics.hyracks.api.util.JavaSerializationUtils;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;

public class StatisticsRuntimeFactory extends AbstractOneInputOneOutputRuntimeFactory {

    private static final long serialVersionUID = 1L;
    private BaseStatistics statsData;
    private IEvaluatorFactory evalFactory;

    public StatisticsRuntimeFactory(int[] projectionList, BaseStatistics statsData, IEvaluatorFactory evalFactory) {
        super(projectionList);
        this.statsData = statsData;
        this.evalFactory = evalFactory;
    }

    @Override
    public String toString() {
        return "collect statistics ";
    }

    @Override
    public AbstractOneInputOneOutputPushRuntime createOneOutputPushRuntime(final RuntimeContext context)
            throws AlgebricksException {
        // these branches are created for performance reasons only!
        if (evalFactory == null) {
            return new AbstractOneInputOneOutputOneFramePushRuntime() {

                private long tupleCount = 0;

                @Override
                public void open() throws HyracksDataException {
                    initAccessAppendRef(context);
                    writer.open();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    tAccess.reset(buffer);
                    int nTuple = tAccess.getTupleCount();
                    this.tupleCount += nTuple;
                    for (int t = 0; t < nTuple; t++) {
                        tRef.reset(tAccess, t);
                        // simply forward data
                        if (projectionList != null) {
                            appendProjectionToFrame(t, projectionList);
                        } else {
                            appendTupleToFrame(t);
                        }
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    statsData.setTupleCount(this.tupleCount);
                    try {
                        context.getHyracksContext().sendMessage(JavaSerializationUtils.serialize(statsData),
                                context.getHyracksContext().getJobletContext().getApplicationContext().getNodeId());

                    } catch (Exception e) {
                        Logger.getLogger(this.getClass().getName()).log(Level.WARNING,
                                "exception while trying to report statistics", e);
                    }
                    super.close();
                }

            };
        } else {
            return new AbstractOneInputOneOutputOneFramePushRuntime() {

                private long tupleCount = 0;
                private IEvaluator eval;
                private ArrayBackedValueStorage evalOutput;

                @Override
                public void open() throws HyracksDataException {
                    if (eval == null) {
                        initAccessAppendRef(context);
                        evalOutput = new ArrayBackedValueStorage();
                        try {
                            eval = evalFactory.createEvaluator(evalOutput);
                        } catch (AlgebricksException ae) {
                            throw new HyracksDataException(ae);
                        }
                    }
                    writer.open();
                }

                @Override
                public void nextFrame(ByteBuffer buffer) throws HyracksDataException {
                    tAccess.reset(buffer);
                    int nTuple = tAccess.getTupleCount();
                    this.tupleCount += nTuple;
                    for (int t = 0; t < nTuple; t++) {
                        tRef.reset(tAccess, t);
                        evalOutput.reset();
                        try {
                            eval.evaluate(tRef);
                            // TODO: add binary Evaluator 
                        } catch (AlgebricksException ae) {
                            throw new HyracksDataException(ae);
                        }

                        // simply forward data
                        if (projectionList != null) {
                            appendProjectionToFrame(t, projectionList);
                        } else {
                            appendTupleToFrame(t);
                        }
                    }
                }

                @Override
                public void close() throws HyracksDataException {
                    statsData.setTupleCount(this.tupleCount);
                    try {
                        context.getHyracksContext().sendMessage(JavaSerializationUtils.serialize(statsData),
                                context.getHyracksContext().getJobletContext().getApplicationContext().getNodeId());

                    } catch (Exception e) {
                        Logger.getLogger(this.getClass().getName()).log(Level.WARNING,
                                "exception while trying to report statistics", e);
                    }
                    super.close();
                }

            };
        }
    }
}
