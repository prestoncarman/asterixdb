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

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import edu.uci.ics.asterix.external.library.java.IJType;
import edu.uci.ics.asterix.external.library.java.JTypes.JInt32;
import edu.uci.ics.asterix.external.library.java.JTypes.JRecord;
import edu.uci.ics.asterix.external.library.java.JTypes.JString;
import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.functions.IExternalFunctionInfo;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.data.std.api.IDataOutputProvider;

public class AdvancedAsterixFunctionArgumentProvider implements IFunctionHelper {

    private final IExternalFunctionInfo finfo;
    private IJType[] arguments;
    private final IDataOutputProvider outputProvider;
    private IJType resultHolder;
    private IAObject innerResult;
    private ISerializerDeserializer resultSerde;

    public AdvancedAsterixFunctionArgumentProvider(IExternalFunctionInfo finfo, IDataOutputProvider outputProvider)
            throws AlgebricksException {
        this.finfo = finfo;
        this.outputProvider = outputProvider;
        List<IAType> params = finfo.getParamList();
        arguments = new IJType[params.size()];
        int index = 0;
        for (IAType param : params) {
            this.arguments[index] = getAObjectArray(param, false);
            index++;
        }
        initResultHolder();
    }

    private IJType getAObjectArray(IAType type, boolean deepCreate) throws AlgebricksException {
        IJType retValue;
        switch (type.getTypeTag()) {
            case INT32:
                retValue = new JInt32();
                break;
            case STRING:
                retValue = new JString();
                break;
            case RECORD:
                if (deepCreate) {
                    IAType[] fieldTypes = ((ARecordType) type).getFieldTypes();
                    IJType[] fieldObjects = new IJType[fieldTypes.length];
                    int index = 0;
                    for (IAType fieldType : fieldTypes) {
                        fieldObjects[index] = getAObjectArray(fieldType, deepCreate);
                        index++;
                    }
                    retValue = new JRecord((ARecordType) type, fieldObjects);
                } else {
                    retValue = new JRecord((ARecordType) type);
                }
                break;
            default:
                throw new AlgebricksException("Unsupported type:" + type.getTypeTag());
        }
        return retValue;
    }

    private void initResultHolder() throws AlgebricksException {
        IAType retType = finfo.getReturnType();
        resultHolder = getAObjectArray(retType, true);
        innerResult = convertIJTypeToIAObject(resultHolder);
        resultSerde = AqlSerializerDeserializerProvider.INSTANCE.getNonTaggedSerializerDeserializer(innerResult
                .getType());
    }

    private IAObject convertIJTypeToIAObject(IJType jtypeObject) throws AlgebricksException {
        ATypeTag resultType = jtypeObject.getTypeTag();
        IAObject retValue = null;
        switch (resultType) {
            case INT32:
                retValue = ((JInt32) jtypeObject).getIAObject();
                break;
            case STRING:
                retValue = ((JString) jtypeObject).getIAObject();
                break;
            case RECORD:
                IJType[] fields = ((JRecord) jtypeObject).getFields();
                int index = 0;
                IAObject[] obj = new IAObject[fields.length];
                for (IJType field : fields) {
                    obj[index] = convertIJTypeToIAObject(field);
                    index++;
                }
                retValue = new ARecord((ARecordType) finfo.getReturnType(), obj);
                break;
            default:
                throw new AlgebricksException("Unsupported type:" + resultType);
        }
        return retValue;
    }

    @Override
    public int getNumberOfArguments() {
        return arguments.length;
    }

    public void setArgument(int index, byte[] argument) throws HyracksDataException {
        ATypeTag typeTag = finfo.getParamList().get(index).getTypeTag();
        switch (typeTag) {
            case INT32:
                int v = valueFromBytes(argument, 1, 4);
                ((JInt32) arguments[index]).setValue(v);
                break;
            case STRING:
                ((JString) arguments[index]).setValue(AStringSerializerDeserializer.INSTANCE.deserialize(
                        new DataInputStream(new ByteArrayInputStream(argument))).getStringValue());
                break;
            case RECORD:
                ((JRecord) arguments[index]).setValue(argument);
                break;
            default:
                throw new IllegalStateException("Argument type: " + finfo.getParamList().get(index).getTypeTag());
        }
    }

    @Override
    public IJType getArgument(int index) {
        return arguments[index];
    }

    private static int valueFromBytes(byte[] bytes, int offset, int length) {
        return ((bytes[offset] & 0xff) << 24) + ((bytes[offset + 1] & 0xff) << 16) + ((bytes[offset + 2] & 0xff) << 8)
                + ((bytes[offset + 3] & 0xff) << 0);
    }

    @Override
    public void setResult(IJType result) throws IOException, AsterixException {
        try {
            outputProvider.getDataOutput().writeByte(innerResult.getType().getTypeTag().serialize());
        } catch (IOException e) {
            throw new HyracksDataException(e);
        }
        resultSerde.serialize(innerResult, outputProvider.getDataOutput());
    }

    @Override
    public IAType getReturnType() {
        return finfo.getReturnType();
    }

    @Override
    public IJType getResultHolder() {
        return resultHolder;
    }

}