package edu.uci.ics.asterix.translator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import edu.uci.ics.asterix.aql.expression.OrderedListTypeDefinition;
import edu.uci.ics.asterix.aql.expression.RecordTypeDefinition;
import edu.uci.ics.asterix.aql.expression.RecordTypeDefinition.RecordKind;
import edu.uci.ics.asterix.aql.expression.TypeDecl;
import edu.uci.ics.asterix.aql.expression.TypeExpression;
import edu.uci.ics.asterix.aql.expression.TypeReferenceExpression;
import edu.uci.ics.asterix.aql.expression.UnorderedListTypeDefinition;
import edu.uci.ics.asterix.common.annotations.IRecordFieldDataGen;
import edu.uci.ics.asterix.common.annotations.RecordDataGenAnnotation;
import edu.uci.ics.asterix.common.annotations.TypeDataGen;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.MetadataManager;
import edu.uci.ics.asterix.metadata.MetadataTransactionContext;
import edu.uci.ics.asterix.metadata.bootstrap.AsterixProperties;
import edu.uci.ics.asterix.metadata.declared.AqlCompiledMetadataDeclarations;
import edu.uci.ics.asterix.metadata.entities.AsterixBuiltinTypeMap;
import edu.uci.ics.asterix.metadata.entities.Datatype;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.AbstractCollectionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.types.TypeSignature;
import edu.uci.ics.hyracks.algebricks.data.IAWriterFactory;
import edu.uci.ics.hyracks.dataflow.std.file.FileSplit;

public final class MetadataDeclTranslator {
    private final MetadataTransactionContext mdTxnCtx;
    private final String defaultDataverse;
    private final List<TypeDecl> typeDeclarations;
    private final FileSplit outputFile;
    private final Map<String, String> config;
    private final IAWriterFactory writerFactory;

    public MetadataDeclTranslator(MetadataTransactionContext mdTxnCtx, String defaultDataverse, FileSplit outputFile,
            IAWriterFactory writerFactory, Map<String, String> config, List<TypeDecl> typeDeclarations) {
        this.mdTxnCtx = mdTxnCtx;
        this.defaultDataverse = defaultDataverse;
        this.outputFile = outputFile;
        this.writerFactory = writerFactory;
        this.config = config;
        this.typeDeclarations = typeDeclarations;
    }

    // TODO: Should this not throw an AsterixException?
    public AqlCompiledMetadataDeclarations computeMetadataDeclarations(boolean online) throws AsterixException,
            MetadataException {
        Map<TypeSignature, TypeDataGen> typeDataGenMap = new HashMap<TypeSignature, TypeDataGen>();
        for (TypeDecl td : typeDeclarations) {
            String typeDataverse = td.getDataverseName() == null ? defaultDataverse : td.getDataverseName().getValue();
            TypeSignature signature = new TypeSignature(typeDataverse, td.getIdent().getValue());
            TypeDataGen tdg = td.getDatagenAnnotation();
            if (tdg != null) {
                typeDataGenMap.put(signature, tdg);
            }
        }
        Map<TypeSignature, IAType> typeMap = computeTypes();
        Map<String, String[]> stores = AsterixProperties.INSTANCE.getStores();
        AqlCompiledMetadataDeclarations compiledDeclarations = new AqlCompiledMetadataDeclarations(mdTxnCtx,
                defaultDataverse, outputFile, config, stores, typeMap, typeDataGenMap, writerFactory, online);
        return compiledDeclarations;
    }

    private Map<TypeSignature, IAType> computeTypes() throws AsterixException, MetadataException {
        Map<TypeSignature, IAType> typeMap = new HashMap<TypeSignature, IAType>();
        Map<TypeSignature, Map<ARecordType, List<Integer>>> incompleteFieldTypes = new HashMap<TypeSignature, Map<ARecordType, List<Integer>>>();
        Map<TypeSignature, List<AbstractCollectionType>> incompleteItemTypes = new HashMap<TypeSignature, List<AbstractCollectionType>>();
        Map<TypeSignature, List<TypeSignature>> incompleteTopLevelTypeReferences = new HashMap<TypeSignature, List<TypeSignature>>();

        firstPass(typeMap, incompleteFieldTypes, incompleteItemTypes, incompleteTopLevelTypeReferences);
        secondPass(typeMap, incompleteFieldTypes, incompleteItemTypes, incompleteTopLevelTypeReferences);
        return typeMap;
    }

    private void secondPass(Map<TypeSignature, IAType> typeMap,
            Map<TypeSignature, Map<ARecordType, List<Integer>>> incompleteFieldTypes,
            Map<TypeSignature, List<AbstractCollectionType>> incompleteItemTypes,
            Map<TypeSignature, List<TypeSignature>> incompleteTopLevelTypeReferences) throws AsterixException,
            MetadataException {
        // solve remaining top level references
        for (TypeSignature typeSignature : incompleteTopLevelTypeReferences.keySet()) {
            IAType t = typeMap.get(typeSignature);
            if (t == null) {
                throw new AsterixException("Could not resolve type " + typeSignature);
            }
            for (TypeSignature sign : incompleteTopLevelTypeReferences.get(typeSignature)) {
                typeMap.put(sign, t);
            }
        }
        // solve remaining field type references
        for (TypeSignature sign : incompleteFieldTypes.keySet()) {
            IAType t = typeMap.get(sign);
            if (t == null) {
                // Try to get type from the metadata manager.
                Datatype metadataDataType = MetadataManager.INSTANCE.getDatatype(mdTxnCtx, sign.getNamespace(),
                        sign.getName());
                if (metadataDataType == null) {
                    throw new AsterixException("Could not resolve type " + sign);
                }
                t = metadataDataType.getDatatype();
                typeMap.put(sign, t);
            }
            Map<ARecordType, List<Integer>> fieldsToFix = incompleteFieldTypes.get(sign);
            for (ARecordType recType : fieldsToFix.keySet()) {
                List<Integer> positions = fieldsToFix.get(recType);
                IAType[] fldTypes = recType.getFieldTypes();
                for (Integer pos : positions) {
                    if (fldTypes[pos] == null) {
                        fldTypes[pos] = t;
                    } else { // nullable
                        AUnionType nullableUnion = (AUnionType) fldTypes[pos];
                        nullableUnion.setTypeAtIndex(t, 1);
                    }
                }
            }
        }
        // solve remaining item type references
        for (TypeSignature sign : incompleteItemTypes.keySet()) {
            IAType t = typeMap.get(sign);
            if (t == null) {
                throw new AsterixException("Could not resolve type " + sign);
            }
            for (AbstractCollectionType act : incompleteItemTypes.get(sign)) {
                act.setItemType(t);
            }
        }
    }

    private void firstPass(Map<TypeSignature, IAType> typeMap,
            Map<TypeSignature, Map<ARecordType, List<Integer>>> incompleteFieldTypes,
            Map<TypeSignature, List<AbstractCollectionType>> incompleteItemTypes,
            Map<TypeSignature, List<TypeSignature>> incompleteTopLevelTypeReferences) throws AsterixException {
        for (TypeDecl td : typeDeclarations) {
            TypeExpression texpr = td.getTypeDef();
            String tdname = td.getIdent().getValue();
            if (AsterixBuiltinTypeMap.getBuiltinTypes().get(tdname) != null) {
                throw new AsterixException("Cannot redefine builtin type " + tdname + " .");
            }
            String typeDataverse = td.getDataverseName() == null ? defaultDataverse : td.getDataverseName().getValue();
            TypeSignature signature = new TypeSignature(typeDataverse, tdname);
            switch (texpr.getTypeKind()) {
                case TYPEREFERENCE: {
                    TypeReferenceExpression tre = (TypeReferenceExpression) texpr;

                    IAType t = solveTypeReference(signature, typeMap);
                    if (t != null) {
                        typeMap.put(signature, t);
                    } else {
                        addIncompleteTopLevelTypeReference(signature, tre, incompleteTopLevelTypeReferences);
                    }
                    break;
                }
                case RECORD: {
                    RecordTypeDefinition rtd = (RecordTypeDefinition) texpr;
                    ARecordType recType = computeRecordType(signature, rtd, typeMap, incompleteFieldTypes,
                            incompleteItemTypes, typeDataverse);
                    typeMap.put(signature, recType);
                    break;
                }
                case ORDEREDLIST: {
                    OrderedListTypeDefinition oltd = (OrderedListTypeDefinition) texpr;
                    AOrderedListType olType = computeOrderedListType(signature, oltd, typeMap, incompleteItemTypes,
                            incompleteFieldTypes, typeDataverse);
                    typeMap.put(signature, olType);
                    break;
                }
                case UNORDEREDLIST: {
                    UnorderedListTypeDefinition ultd = (UnorderedListTypeDefinition) texpr;
                    AUnorderedListType ulType = computeUnorderedListType(signature, ultd, typeMap, incompleteItemTypes,
                            incompleteFieldTypes, typeDataverse);
                    typeMap.put(signature, ulType);
                    break;
                }
                default: {
                    throw new IllegalStateException();
                }
            }
        }
    }

    private AOrderedListType computeOrderedListType(TypeSignature typeSignature, OrderedListTypeDefinition oltd,
            Map<TypeSignature, IAType> typeMap, Map<TypeSignature, List<AbstractCollectionType>> incompleteItemTypes,
            Map<TypeSignature, Map<ARecordType, List<Integer>>> incompleteFieldTypes, String typeDataverse) {
        TypeExpression tExpr = oltd.getItemTypeExpression();
        String typeName = typeSignature != null ? typeSignature.getName() : null;
        AOrderedListType aolt = new AOrderedListType(null, typeName);
        setCollectionItemType(tExpr, typeMap, incompleteItemTypes, incompleteFieldTypes, aolt, typeDataverse);
        return aolt;
    }

    private AUnorderedListType computeUnorderedListType(TypeSignature typeSignature, UnorderedListTypeDefinition ultd,
            Map<TypeSignature, IAType> typeMap, Map<TypeSignature, List<AbstractCollectionType>> incompleteItemTypes,
            Map<TypeSignature, Map<ARecordType, List<Integer>>> incompleteFieldTypes, String typeDataverse) {
        TypeExpression tExpr = ultd.getItemTypeExpression();
        String typeName = typeSignature != null ? typeSignature.getName() : null;
        AUnorderedListType ault = new AUnorderedListType(null, typeName);
        setCollectionItemType(tExpr, typeMap, incompleteItemTypes, incompleteFieldTypes, ault, typeDataverse);
        return ault;
    }

    private void setCollectionItemType(TypeExpression tExpr, Map<TypeSignature, IAType> typeMap,
            Map<TypeSignature, List<AbstractCollectionType>> incompleteItemTypes,
            Map<TypeSignature, Map<ARecordType, List<Integer>>> incompleteFieldTypes, AbstractCollectionType act,
            String typeDataverse) {
        switch (tExpr.getTypeKind()) {
            case ORDEREDLIST: {
                OrderedListTypeDefinition oltd = (OrderedListTypeDefinition) tExpr;
                IAType t = computeOrderedListType(null, oltd, typeMap, incompleteItemTypes, incompleteFieldTypes,
                        typeDataverse);
                act.setItemType(t);
                break;
            }
            case UNORDEREDLIST: {
                UnorderedListTypeDefinition ultd = (UnorderedListTypeDefinition) tExpr;
                IAType t = computeUnorderedListType(null, ultd, typeMap, incompleteItemTypes, incompleteFieldTypes,
                        typeDataverse);
                act.setItemType(t);
                break;
            }
            case RECORD: {
                RecordTypeDefinition rtd = (RecordTypeDefinition) tExpr;
                IAType t = computeRecordType(null, rtd, typeMap, incompleteFieldTypes, incompleteItemTypes,
                        typeDataverse);
                act.setItemType(t);
                break;
            }
            case TYPEREFERENCE: {
                TypeReferenceExpression tre = (TypeReferenceExpression) tExpr;
                TypeSignature signature = new TypeSignature(typeDataverse, tre.getIdent().getValue());
                IAType tref = solveTypeReference(signature, typeMap);
                if (tref != null) {
                    act.setItemType(tref);
                } else {
                    addIncompleteCollectionTypeReference(act, tre, incompleteItemTypes, typeDataverse);
                }
                break;
            }
            default: {
                throw new IllegalStateException();
            }
        }
    }

    private ARecordType computeRecordType(TypeSignature typeSignature, RecordTypeDefinition rtd,
            Map<TypeSignature, IAType> typeMap,
            Map<TypeSignature, Map<ARecordType, List<Integer>>> incompleteFieldTypes,
            Map<TypeSignature, List<AbstractCollectionType>> incompleteItemTypes, String typeDataverse) {
        List<String> names = rtd.getFieldNames();
        int n = names.size();
        String[] fldNames = new String[n];
        IAType[] fldTypes = new IAType[n];
        int i = 0;
        for (String s : names) {
            fldNames[i++] = s;
        }
        boolean isOpen = rtd.getRecordKind() == RecordKind.OPEN;
        ARecordType recType = new ARecordType(typeSignature == null ? null : typeSignature.getName(), fldNames,
                fldTypes, isOpen);

        List<IRecordFieldDataGen> fieldDataGen = rtd.getFieldDataGen();
        if (fieldDataGen.size() == n) {
            IRecordFieldDataGen[] rfdg = new IRecordFieldDataGen[n];
            rfdg = fieldDataGen.toArray(rfdg);
            recType.getAnnotations().add(new RecordDataGenAnnotation(rfdg, rtd.getUndeclaredFieldsDataGen()));
        }

        for (int j = 0; j < n; j++) {
            TypeExpression texpr = rtd.getFieldTypes().get(j);
            switch (texpr.getTypeKind()) {
                case TYPEREFERENCE: {
                    TypeReferenceExpression tre = (TypeReferenceExpression) texpr;
                    TypeSignature signature = new TypeSignature(typeDataverse, tre.getIdent().getValue());

                    IAType tref = solveTypeReference(signature, typeMap);
                    if (tref != null) {
                        if (!rtd.getNullableFields().get(j)) { // not nullable
                            fldTypes[j] = tref;
                        } else { // nullable
                            fldTypes[j] = makeUnionWithNull(null, tref);
                        }
                    } else {
                        addIncompleteFieldTypeReference(recType, j, tre, incompleteFieldTypes,
                                typeSignature.getNamespace());
                        if (rtd.getNullableFields().get(j)) {
                            fldTypes[j] = makeUnionWithNull(null, null);
                        }
                    }
                    break;
                }
                case RECORD: {
                    RecordTypeDefinition recTypeDef2 = (RecordTypeDefinition) texpr;
                    IAType t2 = computeRecordType(null, recTypeDef2, typeMap, incompleteFieldTypes,
                            incompleteItemTypes, typeDataverse);
                    if (!rtd.getNullableFields().get(j)) { // not nullable
                        fldTypes[j] = t2;
                    } else { // nullable
                        fldTypes[j] = makeUnionWithNull(null, t2);
                    }
                    break;
                }
                case ORDEREDLIST: {
                    OrderedListTypeDefinition oltd = (OrderedListTypeDefinition) texpr;
                    IAType t2 = computeOrderedListType(null, oltd, typeMap, incompleteItemTypes, incompleteFieldTypes,
                            typeDataverse);
                    fldTypes[j] = (rtd.getNullableFields().get(j)) ? makeUnionWithNull(null, t2) : t2;
                    break;
                }
                case UNORDEREDLIST: {
                    UnorderedListTypeDefinition ultd = (UnorderedListTypeDefinition) texpr;
                    IAType t2 = computeUnorderedListType(null, ultd, typeMap, incompleteItemTypes,
                            incompleteFieldTypes, typeDataverse);
                    fldTypes[j] = (rtd.getNullableFields().get(j)) ? makeUnionWithNull(null, t2) : t2;
                    break;
                }
                default: {
                    throw new IllegalStateException();
                }
            }

        }

        return recType;
    }

    private AUnionType makeUnionWithNull(String unionTypeName, IAType type) {
        ArrayList<IAType> unionList = new ArrayList<IAType>(2);
        unionList.add(BuiltinType.ANULL);
        unionList.add(type);
        return new AUnionType(unionList, unionTypeName);
    }

    private void addIncompleteCollectionTypeReference(AbstractCollectionType collType, TypeReferenceExpression tre,
            Map<TypeSignature, List<AbstractCollectionType>> incompleteItemTypes, String typeDataverse) {
        String typeName = tre.getIdent().getValue();
        List<AbstractCollectionType> typeList = incompleteItemTypes.get(typeName);
        TypeSignature typeSignature = new TypeSignature(typeDataverse, typeName);

        if (typeList == null) {
            typeList = new LinkedList<AbstractCollectionType>();
            incompleteItemTypes.put(typeSignature, typeList);
        }
        typeList.add(collType);
    }

    private void addIncompleteFieldTypeReference(ARecordType recType, int fldPosition, TypeReferenceExpression tre,
            Map<TypeSignature, Map<ARecordType, List<Integer>>> incompleteFieldTypes, String typeDataverse) {
        String typeName = tre.getIdent().getValue();
        Map<ARecordType, List<Integer>> refMap = incompleteFieldTypes.get(typeName);
        if (refMap == null) {
            refMap = new HashMap<ARecordType, List<Integer>>();
            incompleteFieldTypes.put(new TypeSignature(typeDataverse, typeName), refMap);
        }
        List<Integer> typeList = refMap.get(recType);
        if (typeList == null) {
            typeList = new ArrayList<Integer>();
            refMap.put(recType, typeList);
        }
        typeList.add(fldPosition);
    }

    private void addIncompleteTopLevelTypeReference(TypeSignature typeSignature, TypeReferenceExpression tre,
            Map<TypeSignature, List<TypeSignature>> incompleteTopLevelTypeReferences) {
        List<TypeSignature> refList = incompleteTopLevelTypeReferences.get(typeSignature);
        if (refList == null) {
            refList = new LinkedList<TypeSignature>();
            incompleteTopLevelTypeReferences.put(typeSignature, refList);
        }
        refList.add(typeSignature);
    }

    private IAType solveTypeReference(TypeSignature signature, Map<TypeSignature, IAType> typeMap) {
        String name = signature.getName();
        IAType builtin = AsterixBuiltinTypeMap.getBuiltinTypes().get(name);
        if (builtin != null) {
            return builtin;
        } else {
            return typeMap.get(signature);
        }
    }
}
