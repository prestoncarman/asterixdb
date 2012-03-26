package edu.uci.ics.asterix.runtime.evaluators.common;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryHashFunctionFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AFloat;
import edu.uci.ics.asterix.om.base.AMutableFloat;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.runtime.evaluators.functions.BinaryHashMap;
import edu.uci.ics.asterix.runtime.evaluators.functions.BinaryHashMap.BinaryEntry;
import edu.uci.ics.fuzzyjoin.similarity.SimilarityMetricJaccard;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluator;
import edu.uci.ics.hyracks.algebricks.core.algebra.runtime.base.IEvaluatorFactory;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.utils.Pair;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparator;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryHashFunction;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.primitive.IntegerPointable;
import edu.uci.ics.hyracks.data.std.primitive.UTF8StringPointable;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ArrayBackedValueStorage;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IDataOutputProvider;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.IFrameTupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;

public class SimilarityJaccardEvaluator implements IEvaluator {

	protected final int TABLE_SIZE = 100;
	protected final int TABLE_FRAME_SIZE = 32768;
	
    // Assuming type indicator in serde format.
    protected final int typeIndicatorSize = 1;

    protected final DataOutput out;
    protected final ArrayBackedValueStorage argOut = new ArrayBackedValueStorage();
    protected final IEvaluator firstOrdListEval;
    protected final IEvaluator secondOrdListEval;

    protected final AsterixOrderedListIterator fstOrdListIter = new AsterixOrderedListIterator();
    protected final AsterixOrderedListIterator sndOrdListIter = new AsterixOrderedListIterator();
    protected final AsterixUnorderedListIterator fstUnordListIter = new AsterixUnorderedListIterator();
    protected final AsterixUnorderedListIterator sndUnordListIter = new AsterixUnorderedListIterator();

    protected AbstractAsterixListIterator firstListIter;
    protected AbstractAsterixListIterator secondListIter;

    protected final SimilarityMetricJaccard jaccard = new SimilarityMetricJaccard();
    protected final AMutableFloat aFloat = new AMutableFloat(0);
    @SuppressWarnings("unchecked")
    protected final ISerializerDeserializer<AFloat> floatSerde = AqlSerializerDeserializerProvider.INSTANCE
            .getSerializerDeserializer(BuiltinType.AFLOAT);

    protected ATypeTag firstTypeTag;
    protected ATypeTag secondTypeTag;
    protected int firstStart = -1;
    protected int secondStart = -1;
    protected float jaccSim = 0.0f;
    protected ATypeTag itemTypeTag;
    
    protected BinaryHashMap hashMap;
    protected BinaryEntry keyEntry = new BinaryEntry();
    protected BinaryEntry valEntry = new BinaryEntry();
    
    public SimilarityJaccardEvaluator(IEvaluatorFactory[] args, IDataOutputProvider output) throws AlgebricksException {
        out = output.getDataOutput();
        firstOrdListEval = args[0].createEvaluator(argOut);
        secondOrdListEval = args[1].createEvaluator(argOut);
        byte[] emptyValBuf = new byte[8];
        Arrays.fill(emptyValBuf, (byte)0);
        valEntry.set(emptyValBuf, 0, 8);
    }

    @Override
    public void evaluate(IFrameTupleReference tuple) throws AlgebricksException {
        runArgEvals(tuple);
        if (!checkArgTypes(firstTypeTag, secondTypeTag)) {
            return;
        }
        jaccSim = computeResult(argOut.getBytes(), firstStart, secondStart, firstTypeTag);
        try {
            writeResult(jaccSim);
        } catch (IOException e) {
            throw new AlgebricksException(e);
        }
    }

    protected void runArgEvals(IFrameTupleReference tuple) throws AlgebricksException {
        argOut.reset();

        firstStart = argOut.getLength();
        firstOrdListEval.evaluate(tuple);
        secondStart = argOut.getLength();
        secondOrdListEval.evaluate(tuple);

        firstTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut.getBytes()[firstStart]);
        secondTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(argOut.getBytes()[secondStart]);
    }

    protected float computeResult(byte[] bytes, int firstStart, int secondStart, ATypeTag argType)
            throws AlgebricksException {
        firstListIter.reset(bytes, firstStart);
        secondListIter.reset(bytes, secondStart);
        // Check for special case where one of the lists is empty, since list
        // types won't match.
        int firstListSize = firstListIter.size();
        int secondListSize = secondListIter.size();
        if (firstListSize == 0 || secondListSize == 0) {
            return 0.0f;
        }
        if (firstTypeTag == ATypeTag.ANY || secondTypeTag == ATypeTag.ANY) {
            throw new AlgebricksException("\n Jaccard can only be called on homogenous lists");
        }
    	// TODO: Check item types are compatible.
        itemTypeTag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(bytes[firstStart + 1]);
        
        setHashMap(bytes, firstStart, secondStart);
        
        // We will subtract the intersection size later to get the real union size.
        int unionSize = firstListSize + secondListSize;
        int intersectionSize = 0;
        
        // Add items into hash map, starting with first list.
        // Value in map is a pair of integers. Set first integer to 1.
    	IntegerPointable.setInteger(valEntry.buf, 0, 1);
        while (firstListIter.hasNext()) {        	
        	byte[] buf = firstListIter.getData();        	
        	int off = firstListIter.getPos();
        	int len = getItemLen(buf, off);
        	keyEntry.set(buf, off, len);        	
        	BinaryEntry entry = hashMap.put(keyEntry, valEntry);
        	if (entry != null) {
        		// Increment value.
        		int firstValInt = IntegerPointable.getInteger(buf, 0);
        		IntegerPointable.setInteger(entry.buf, 0, firstValInt + 1);
        	}
        	firstListIter.next();
        }
        
        // Add items from second map.
        IntegerPointable.setInteger(valEntry.buf, 0, 0);
        IntegerPointable.setInteger(valEntry.buf, 4, 1);
        while (secondListIter.hasNext()) {        	
        	byte[] buf = secondListIter.getData();
        	int off = secondListIter.getPos();
        	int len = getItemLen(buf, off);
        	keyEntry.set(buf, off, len);        	
        	BinaryEntry entry = hashMap.get(keyEntry);
        	if (entry != null) {
        		// Increment second value.
        		int firstValInt = IntegerPointable.getInteger(buf, 0);
        		// Irrelevant for the intersection size.
        		if (firstValInt == 0) {
        			continue;
        		}
        		int secondValInt = IntegerPointable.getInteger(buf, 4);
        		// Subtract old min value.
        		intersectionSize -= (firstValInt < secondValInt) ? firstValInt : secondValInt;
        		secondValInt++;
        		// Add new min value.
        		intersectionSize += (firstValInt < secondValInt) ? firstValInt : secondValInt;
        		IntegerPointable.setInteger(entry.buf, 0, secondValInt);
        	}
        	secondListIter.next();
        }
        unionSize -= intersectionSize;
        return (float) intersectionSize / (float) unionSize;
    }

    protected void setHashMap(byte[] bytes, int firstStart, int secondStart) {
    	if (hashMap != null) {
    		hashMap.clear();
    		return;
    	}
    	IBinaryHashFunction hashFunc = null;
    	IBinaryComparator cmp = null;
    	switch (itemTypeTag) {
    	case INT32: {
    		hashFunc = AqlBinaryHashFunctionFactoryProvider.INTEGER_POINTABLE_INSTANCE.createBinaryHashFunction();
    		cmp = AqlBinaryComparatorFactoryProvider.INTEGER_POINTABLE_INSTANCE.createBinaryComparator();
    		break;
    	}
    	case FLOAT: {
    		hashFunc = AqlBinaryHashFunctionFactoryProvider.FLOAT_POINTABLE_INSTANCE.createBinaryHashFunction();
    		cmp = AqlBinaryComparatorFactoryProvider.FLOAT_POINTABLE_INSTANCE.createBinaryComparator();
    		break;
    	}
    	case DOUBLE: {
    		hashFunc = AqlBinaryHashFunctionFactoryProvider.DOUBLE_POINTABLE_INSTANCE.createBinaryHashFunction();
    		cmp = AqlBinaryComparatorFactoryProvider.DOUBLE_POINTABLE_INSTANCE.createBinaryComparator();
    		break;
    	}
    	case STRING: {
    		hashFunc = AqlBinaryHashFunctionFactoryProvider.UTF8STRING_POINTABLE_INSTANCE.createBinaryHashFunction();
    		cmp = AqlBinaryComparatorFactoryProvider.UTF8STRING_POINTABLE_INSTANCE.createBinaryComparator();
    		break;
    	}
    	default: {
    		break;
    	}
    	}
    	hashMap = new BinaryHashMap(TABLE_SIZE, TABLE_FRAME_SIZE, hashFunc, cmp);
    }
    
    protected int getItemLen(byte[] bytes, int itemOff) {
    	switch (itemTypeTag) {
    	case INT32: {
    		return 4;
    	}
    	case FLOAT: {
    		return 4;
    	}
    	case DOUBLE: {
    		return 8;
    	}
    	case STRING: {
    		// 2 bytes for the UTF8 len, plus the string data.
    		return 2 + UTF8StringPointable.getUTFLen(bytes, itemOff);
    	}
    	default: {
    		return -1;
    	}
    	}
    }
    
    protected boolean checkArgTypes(ATypeTag typeTag1, ATypeTag typeTag2) throws AlgebricksException {
        // jaccard between null and anything else is 0
        if (typeTag1 == ATypeTag.NULL || typeTag2 == ATypeTag.NULL) {
            try {
                writeResult(0.0f);
            } catch (IOException e) {
                throw new AlgebricksException(e);
            }
            return false;
        }
        switch (typeTag1) {
            case ORDEREDLIST: {
                firstListIter = fstOrdListIter;
                break;
            }
            case UNORDEREDLIST: {
                firstListIter = fstUnordListIter;
                break;
            }
            default: {
                throw new AlgebricksException("Invalid types " + typeTag1 + " given as arguments to jaccard.");
            }
        }
        switch (typeTag2) {
            case ORDEREDLIST: {
                secondListIter = sndOrdListIter;
                break;
            }
            case UNORDEREDLIST: {
                secondListIter = sndUnordListIter;
                break;
            }
            default: {
                throw new AlgebricksException("Invalid types " + typeTag2 + " given as arguments to jaccard.");
            }
        }
        return true;
    }

    protected void writeResult(float jacc) throws IOException {
        aFloat.setValue(jacc);
        floatSerde.serialize(aFloat, out);
    }
}