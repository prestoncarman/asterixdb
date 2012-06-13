package edu.uci.ics.asterix.runtime.evaluators.functions;

import java.io.IOException;

import edu.uci.ics.asterix.common.functions.FunctionConstants;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ABoolean;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptor;
import edu.uci.ics.asterix.om.functions.IFunctionDescriptorFactory;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.evaluators.base.AbstractScalarFunctionDynamicDescriptor;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.algebra.functions.FunctionIdentifier;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.BooleanSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class EditDistanceStringIsFilterable extends AbstractScalarFunctionDynamicDescriptor {

    private static final long serialVersionUID = 1L;
    private final static FunctionIdentifier FID = new FunctionIdentifier(FunctionConstants.ASTERIX_NS,
            "edit-distance-string-is-filterable", 4, true);
    public static final IFunctionDescriptorFactory FACTORY = new IFunctionDescriptorFactory() {
        public IFunctionDescriptor createFunctionDescriptor() {
            return new EditDistanceStringIsFilterable();
        }
    };

    @Override
    public IEvaluatorFactory createEvaluatorFactory(final IEvaluatorFactory[] args) throws AlgebricksException {
        return new IEvaluatorFactory() {
            private static final long serialVersionUID = 1L;

            @Override
            public IEvaluator createEvaluator(IDataOutputProvider output) throws AlgebricksException {
                return new EditDistanceStringIsFilterableEvaluator(args, output);
            }
        };
    }

    @Override
    public FunctionIdentifier getIdentifier() {
        return FID;
    }

    private static class EditDistanceStringIsFilterableEvaluator implements IEvaluator {
    	
        protected final ArrayBackedValueStorage argBuf = new ArrayBackedValueStorage();
        protected final IDataOutputProvider output;
        
        protected final IEvaluator stringEval;
        protected final IEvaluator edThreshEval;
        protected final IEvaluator gramLenEval;
        protected final IEvaluator usePrePostEval;        
    	
        protected int strLen;
        protected int edThresh;
        protected int gramLen;
        protected boolean usePrePost;

        @SuppressWarnings("unchecked")
        private final ISerializerDeserializer<ABoolean> booleanSerde = AqlSerializerDeserializerProvider.INSTANCE
                .getSerializerDeserializer(BuiltinType.ABOOLEAN);

        public EditDistanceStringIsFilterableEvaluator(IEvaluatorFactory[] args, IDataOutputProvider output)
                throws AlgebricksException {
            this.output = output;
            stringEval = args[0].createEvaluator(argBuf);
        	edThreshEval = args[1].createEvaluator(argBuf);
        	gramLenEval = args[2].createEvaluator(argBuf);
        	usePrePostEval = args[3].createEvaluator(argBuf);
        }

		@Override
		public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
			ATypeTag typeTag = null;
			
			// Check type and compute string length.
			argBuf.reset();
			stringEval.evaluate(tuple);
			typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argBuf.getByteArray()[0]);
			if (!typeTag.equals(ATypeTag.STRING)) {
				throw new AlgebricksException("Expected type 'STRING' as first argument. Encountered '" + typeTag.toString() + "'.");
			}
			int utf8Length = UTF8StringPointable.getUTFLen(argBuf.getByteArray(), 1); 
			int pos = 3;
			strLen = 0;	        
	        int end = pos + utf8Length;
	        while (pos < end) {
	        	strLen++;
	            pos += UTF8StringPointable.charSize(argBuf.getByteArray(), pos);
	        }
			
	        // Check type and extract edit-distance threshold.
	        argBuf.reset();
	        edThreshEval.evaluate(tuple);
			typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argBuf.getByteArray()[0]);
			if (!typeTag.equals(ATypeTag.INT32)) {
				throw new AlgebricksException("Expected type 'INT32' as second argument. Encountered '" + typeTag.toString() + "'.");
			}
			edThresh = IntegerSerializerDeserializer.getInt(argBuf.getByteArray(), 1);
			
	        // Check type and extract gram length.
			argBuf.reset();
			gramLenEval.evaluate(tuple);
			typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argBuf.getByteArray()[0]);
			if (!typeTag.equals(ATypeTag.INT32)) {
				throw new AlgebricksException("Expected type 'INT32' as third argument. Encountered '" + typeTag.toString() + "'.");
			}
			gramLen = IntegerSerializerDeserializer.getInt(argBuf.getByteArray(), 1);
			
			// Check type and extract usePrePost flag.
			argBuf.reset();
			usePrePostEval.evaluate(tuple);
			typeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argBuf.getByteArray()[0]);
			if (!typeTag.equals(ATypeTag.BOOLEAN)) {
				throw new AlgebricksException("Expected type 'BOOLEAN' as fourth argument. Encountered '" + typeTag.toString() + "'.");
			}
			usePrePost = BooleanSerializerDeserializer.getBoolean(argBuf.getByteArray(), 1);
			
			// Compute result.			
            int numGrams = (usePrePost) ? strLen + gramLen - 1 : strLen - gramLen + 1;
            int lowerBound = numGrams - edThresh * gramLen;
            try {
                if (lowerBound <= 0 || strLen == 0) {
                    booleanSerde.serialize(ABoolean.FALSE, output.getDataOutput());
                } else {
                    booleanSerde.serialize(ABoolean.TRUE, output.getDataOutput());
                }
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
		}
    }
}
