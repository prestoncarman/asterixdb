/*
 * Copyright 2009-2012 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.asterix.external.library;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.List;

import edu.uci.ics.asterix.builders.RecordBuilder;
import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import edu.uci.ics.asterix.external.library.java.IJObject;
import edu.uci.ics.asterix.external.library.java.JTypeTag;
import edu.uci.ics.asterix.external.library.java.JTypes.JInt;
import edu.uci.ics.asterix.external.library.java.JTypes.JRecord;
import edu.uci.ics.asterix.external.library.java.JTypes.JString;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AMutableRecord;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.AString;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.functions.IExternalFunctionInfo;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.container.IObjectPool;
import edu.uci.ics.asterix.om.util.container.ListObjectPool;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;
import edu.uci.ics.hyracks.data.std.util.ArrayBackedValueStorage;

public class JavaFunctionHelper implements IFunctionHelper {

    private final IExternalFunctionInfo finfo;
    private final IDataOutputProvider outputProvider;
    private IJObject[] arguments;
    private IJObject resultHolder;
    private IAObject innerResult;
    private ISerializerDeserializer resultSerde;
    private IObjectPool<IJObject, IAType> objectPool = new ListObjectPool<IJObject, IAType>(new JTypeObjectFactory());

    public JavaFunctionHelper(IExternalFunctionInfo finfo, IDataOutputProvider outputProvider)
            throws AlgebricksException {
        this.finfo = finfo;
        this.outputProvider = outputProvider;
        List<IAType> params = finfo.getParamList();
        arguments = new IJObject[params.size()];
        int index = 0;
        for (IAType param : params) {
            this.arguments[index] = objectPool.allocate(param);
            index++;
        }
        resultHolder = objectPool.allocate(finfo.getReturnType());
        innerResult = convertIJTypeToIAObject(resultHolder);
        resultSerde = AqlSerializerDeserializerProvider.INSTANCE.getNonTaggedSerializerDeserializer(innerResult
                .getType());
    }

    @Override
    public IJObject getArgument(int index) {
        return arguments[index];
    }

    @Override
    public void setResult(IJObject result) throws IOException, AsterixException {
        IAObject obj = result.getIAObject();
        try {
            //    outputProvider.getDataOutput().writeByte(innerResult.getType().getTypeTag().serialize());
            outputProvider.getDataOutput().writeByte(obj.getType().getTypeTag().serialize());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }

        if (obj.getType().getTypeTag().equals(ATypeTag.RECORD)) {
            ARecordType recType = (ARecordType) obj.getType();
            if (recType.isOpen()) {
                writeOpenRecord((JRecord) result);
            } else {
                resultSerde = AqlSerializerDeserializerProvider.INSTANCE.getNonTaggedSerializerDeserializer(recType);
                resultSerde.serialize(obj, outputProvider.getDataOutput());
            }
        } else {
            resultSerde.serialize(obj, outputProvider.getDataOutput());
        }
        reset();
    }

    private void writeOpenRecord(JRecord jRecord) throws AsterixException, IOException {
        ARecord aRecord = (ARecord) jRecord.getIAObject();
        RecordBuilder recordBuilder = new RecordBuilder();
        ARecordType recordType = aRecord.getType();
        recordBuilder.reset(recordType);
        ArrayBackedValueStorage fieldName = new ArrayBackedValueStorage();
        ArrayBackedValueStorage fieldValue = new ArrayBackedValueStorage();
        List<Boolean> openField = jRecord.getOpenField();

        int fieldIndex = 0;
        int closedFieldId = 0;
        for (IJObject field : jRecord.getFields()) {
            fieldValue.reset();
            switch (field.getTypeTag()) {
                case INT32:
                    AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.AINT32)
                            .serialize(field.getIAObject(), fieldValue.getDataOutput());
                    break;
                case STRING:
                    AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING).serialize(
                            field.getIAObject(), fieldValue.getDataOutput());
                    break;
            }
            if (openField.get(fieldIndex)) {
                String fName = jRecord.getFieldNames().get(fieldIndex);
                fieldName.reset();
                AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(BuiltinType.ASTRING).serialize(
                        new AString(fName), fieldName.getDataOutput());
                recordBuilder.addField(fieldName, fieldValue);
            } else {
                recordBuilder.addField(closedFieldId, fieldValue);
                closedFieldId++;
            }
            fieldIndex++;
        }

        recordBuilder.write(outputProvider.getDataOutput(), false);

    }

    private void reset() {
        for (IJObject jObject : arguments) {
            switch (jObject.getTypeTag()) {
                case RECORD:
                    reset((JRecord) jObject);
                    break;
            }
        }
        switch (resultHolder.getTypeTag()) {
            case RECORD:
                reset((JRecord) resultHolder);
                break;
        }
    }

    private void reset(JRecord jRecord) {
        List<IJObject> fields = ((JRecord) jRecord).getFields();
        for (IJObject field : fields) {
            switch (field.getTypeTag()) {
                case RECORD:
                    reset((JRecord) field);
                    break;
            }
        }
        jRecord.close();
    }

    public void setArgument(int index, byte[] argument) throws HyracksDataException {
        ATypeTag typeTag = finfo.getParamList().get(index).getTypeTag();
        switch (typeTag) {
            case INT32:
                int v = valueFromBytes(argument, 1, 4);
                ((JInt) arguments[index]).setValue(v);
                break;
            case STRING:
                ((JString) arguments[index]).setValue(AStringSerializerDeserializer.INSTANCE.deserialize(
                        new DataInputStream(new ByteArrayInputStream(argument))).getStringValue());
                break;
            case RECORD:
                ((JRecord) arguments[index]).setValue(argument);
                break;
            default:
                throw new IllegalStateException("Invalid argument type: "
                        + finfo.getParamList().get(index).getTypeTag());
        }
    }

    private static int valueFromBytes(byte[] bytes, int offset, int length) {
        return ((bytes[offset] & 0xff) << 24) + ((bytes[offset + 1] & 0xff) << 16) + ((bytes[offset + 2] & 0xff) << 8)
                + ((bytes[offset + 3] & 0xff) << 0);
    }

    private IAObject convertIJTypeToIAObject(IJObject jtypeObject) throws AlgebricksException {
        ATypeTag resultType = jtypeObject.getTypeTag();
        IAObject retValue = null;
        switch (resultType) {
            case INT32:
                retValue = ((JInt) jtypeObject).getIAObject();
                break;
            case STRING:
                retValue = ((JString) jtypeObject).getIAObject();
                break;
            case RECORD:
                List<IJObject> fields = ((JRecord) jtypeObject).getFields();
                int index = 0;
                IAObject[] obj = new IAObject[fields.size()];
                for (IJObject field : fields) {
                    obj[index] = convertIJTypeToIAObject(field);
                    index++;
                }
                retValue = new AMutableRecord((ARecordType) finfo.getReturnType(), obj);
                break;
            default:
                throw new AlgebricksException("Unsupported type:" + resultType);
        }
        return retValue;
    }

    @Override
    public IJObject getResultObject() {
        return resultHolder;
    }

    @Override
    public IJObject getObject(JTypeTag jtypeTag) {
        IJObject retValue = null;
        switch (jtypeTag) {
            case INT:
                retValue = objectPool.allocate(BuiltinType.AINT32);
                break;
            case STRING:
                retValue = objectPool.allocate(BuiltinType.ASTRING);
                break;
        }
        return retValue;
    }

}