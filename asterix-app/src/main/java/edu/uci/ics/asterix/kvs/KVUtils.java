package edu.uci.ics.asterix.kvs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.aql.expression.TypeDecl;
import edu.uci.ics.asterix.formats.nontagged.AqlBinaryComparatorFactoryProvider;
import edu.uci.ics.asterix.formats.nontagged.AqlTypeTraitProvider;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.declared.AqlMetadataProvider;
import edu.uci.ics.asterix.metadata.entities.Dataset;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.metadata.entities.InternalDatasetDetails;
import edu.uci.ics.asterix.metadata.utils.DatasetUtils;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.transaction.management.exception.ACIDException;
import edu.uci.ics.asterix.translator.MetadataDeclTranslator;
import edu.uci.ics.hyracks.algebricks.common.constraints.AlgebricksPartitionConstraint;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.utils.Pair;
import edu.uci.ics.hyracks.algebricks.common.utils.Triple;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.ScalarFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.operators.logical.OrderOperator.IOrder.OrderKind;
import edu.uci.ics.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.dataflow.std.file.IFileSplitProvider;
import edu.uci.ics.hyracks.storage.am.common.api.ITreeIndexFrameFactory;

public class KVUtils {
	public static final String DELAY_PARAM_TAG = "delay";
	public static final String LIMIT_PARAM_TAG = "limit";
	public static final long DEFAULT_DELAY = 1000;
	public static final int DEFAULT_LIMIT = -1;
	
	public enum KVResponseType{
		REGULAR,
		EMPTY,
		MESSAGE
	}
	
	public static AqlCompiledMetadataDeclarations generateACMD(MetadataTransactionContext mdTxnCtx, String dataverseName) throws MetadataException, AlgebricksException{
		List<TypeDecl> typeDeclarations = new ArrayList<TypeDecl>();
        Map<String, String> config = new HashMap<String, String>();
            
        MetadataDeclTranslator metadataTranslator = new MetadataDeclTranslator(mdTxnCtx, dataverseName, null,
                null, config, typeDeclarations);
        
        return metadataTranslator.computeMetadataDeclarations(true);
	}
	
	/*
	public static AqlCompiledDatasetDecl generateACDD(AqlCompiledMetadataDeclarations acmd, String datasetName){
		return acmd.findDataset(datasetName);
	}
	
	
	public static ARecordType getItemType(AqlCompiledMetadataDeclarations acmd, AqlCompiledDatasetDecl acdd) throws AlgebricksException{
		String itemTypeName = acdd.getItemTypeName();
        IAType itemType;
        try {
            itemType = acmd.findType(itemTypeName);
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
        return ((ARecordType) itemType);
	}
	
	public static Triple<ITypeTraits[], IBinaryComparatorFactory[], ISerializerDeserializer[]> getTreeOutputRec(AqlCompiledMetadataDeclarations acmd, AqlCompiledDatasetDecl acdd) throws AlgebricksException{
		//Look @ AqlMetadaProvider.buildBtreeRuntime(...)
		
		String itemTypeName = acdd.getItemTypeName();
        IAType itemType;
        try {
            itemType = acmd.findType(itemTypeName);
        } catch (Exception e) {
            throw new AlgebricksException(e);
        }
        
		int numPrimaryKeys = DatasetUtils.getPartitioningFunctions(acdd).size();
		
		ITypeTraits[] typeTraits = new ITypeTraits[numPrimaryKeys + 1];
        ISerializerDeserializer[] recordFields = new ISerializerDeserializer[numPrimaryKeys + 1];
        IBinaryComparatorFactory[] comparatorFactories = new IBinaryComparatorFactory[numPrimaryKeys];
        
        ISerializerDeserializer payloadSerde = acmd.getFormat().getSerdeProvider().getSerializerDeserializer(itemType);	//It is basically ARecordType SerDe
        recordFields[numPrimaryKeys] = payloadSerde;	
        typeTraits[numPrimaryKeys] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(itemType);
        
        int i = 0;
        for (Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> evalFactoryAndType : DatasetUtils.getPartitioningFunctions(acdd)) {
            IAType keyType = evalFactoryAndType.third;
            ISerializerDeserializer keySerde = acmd.getFormat().getSerdeProvider()
                    .getSerializerDeserializer(keyType);
            recordFields[i] = keySerde;    
            comparatorFactories[i] = AqlBinaryComparatorFactoryProvider.INSTANCE.getBinaryComparatorFactory(keyType, true);
            typeTraits[i] = AqlTypeTraitProvider.INSTANCE.getTypeTrait(keyType);
            ++i;
        }
        
        return new Triple<ITypeTraits[], IBinaryComparatorFactory[], ISerializerDeserializer[]>(typeTraits, comparatorFactories, recordFields);
	}
	
	public static ITreeIndexFrameFactory getInteriorFrameFactory(ITypeTraits[] typeTraits){
		return AqlMetadataProvider.createBTreeNSMInteriorFrameFactory(typeTraits);
	}
	
	public static ITreeIndexFrameFactory getLeafFrameFactory(ITypeTraits[] typeTraits){
		return AqlMetadataProvider.createBTreeNSMLeafFrameFactory(typeTraits);
	}
	
	public static Pair<IFileSplitProvider, AlgebricksPartitionConstraint> getFileSpltAndConstraint(AqlCompiledMetadataDeclarations acmd, String datasetName, String targetIdxName) throws MetadataException, AlgebricksException{
		return acmd.splitProviderAndPartitionConstraintsForInternalOrFeedDataset(datasetName, targetIdxName);
	}
	
	public static String getPixName(AqlCompiledDatasetDecl acdd){
		return DatasetUtils.getPrimaryIndex(acdd).getIndexName();
	}
	
	public static IAType[] getKeyTypes(AqlCompiledDatasetDecl acdd) throws MetadataException{
		AqlCompiledInternalDatasetDetails id = (AqlCompiledInternalDatasetDetails) acdd.getAqlCompiledDatasetDetails();
		List<Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType>> pfList = id.getPartitioningFunctions();
		
		int i=0;
		IAType[] keyTypes = new IAType[pfList.size()];
		for(Triple<IEvaluatorFactory, ScalarFunctionCallExpression, IAType> t : pfList){
			keyTypes[i] = t.third;
			i++;
		}
		return keyTypes;
	}
	
	public static List<String> getPartitioningKeys(AqlCompiledDatasetDecl acdd){
		AqlCompiledInternalDatasetDetails compInternalDsDetails = ((AqlCompiledInternalDatasetDetails) (acdd.getAqlCompiledDatasetDetails()));
		return compInternalDsDetails.getPartitioningExprs();
	}
	*/
	
}
