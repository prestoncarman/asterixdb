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
 */package edu.uci.ics.asterix.runtime.external;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.List;

import edu.uci.ics.asterix.formats.nontagged.AqlSerializerDeserializerProvider;
import edu.uci.ics.asterix.om.base.AOrderedList;
import edu.uci.ics.asterix.om.base.ARecord;
import edu.uci.ics.asterix.om.base.IAObject;
import edu.uci.ics.asterix.om.functions.IExternalFunctionInfo;
import edu.uci.ics.asterix.om.types.IAType;

public class AsterixFunctionArgumentProvider implements IArgumentProvider {

    private final IExternalFunctionInfo finfo;
    private ByteBuffer[] reusableBuffers;
    private IAObject[] reusableObjects;
    private IAType[] paramTypes;

    public AsterixFunctionArgumentProvider(IExternalFunctionInfo finfo) {
        this.finfo = finfo;
        allocateArgumentObjects();
    }

    private void allocateArgumentObjects() {
        List<IAType> params = finfo.getParamList();
        reusableBuffers = new ByteBuffer[params.size()];
        reusableObjects = new IAObject[params.size()];
        paramTypes = new IAType[params.size()];
        int index = 0;
        for (IAType param : params) {
            reusableBuffers[index] = RuntimeExternalFunctionUtil.allocateArgumentBuffers(param);
            reusableObjects[index] = RuntimeExternalFunctionUtil.allocateArgumentObjects(param);
            paramTypes[index] = param;
            index++;
        }
    }

    public int getIntArgument(int index) {
        ByteBuffer reusableArg = (ByteBuffer) reusableBuffers[index];
        int result = reusableArg.getInt(0);
        reusableArg.flip();
        return result;
    }

    public long getLongArgument(int index) {
        ByteBuffer reusableArg = (ByteBuffer) reusableBuffers[index];
        long result = reusableArg.getLong(0);
        reusableArg.flip();
        return result;
    }

    @Override
    public float getFloatArgument(int index) {
        ByteBuffer reusableArg = (ByteBuffer) reusableBuffers[index];
        float result = reusableArg.getFloat(0);
        reusableArg.flip();
        return result;
    }

    public double getDoubleArgument(int index) {
        ByteBuffer reusableArg = (ByteBuffer) reusableBuffers[index];
        double result = reusableArg.getDouble(0);
        reusableArg.flip();
        return result;
    }

    public String getStringArgument(int index) {
        int strlen = reusableBuffers[index].limit();
        int startPos = 0;
        String s = new String(reusableBuffers[index].array(), startPos, strlen);
        return s;
    }

    public AOrderedList getListArgument(int index) throws Exception {
        DataInput in = new DataInputStream(new ByteArrayInputStream(reusableBuffers[index].array()));
        AOrderedList list = (AOrderedList) AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(
                paramTypes[index]).deserialize(in);
        return list;
    }

    public ARecord getRecordArgument(int index) throws Exception {
        DataInput in = new DataInputStream(new ByteArrayInputStream(reusableBuffers[index].array()));
        ARecord record = (ARecord) AqlSerializerDeserializerProvider.INSTANCE.getSerializerDeserializer(
                paramTypes[index]).deserialize(in);
        return record;
    }

    @Override
    public int getNumberOfArguments() {
        return reusableBuffers.length;
    }

    public void setArgument(int index, byte[] argument) {
        int bytesToCopy = 0;
        int startPos = 0;
        switch (paramTypes[index].getTypeTag()) {
            case INT32:
                startPos = 1;
                bytesToCopy = 4;
                break;
            case FLOAT:
                startPos = 1;
                bytesToCopy = 8;
                break;
            case STRING:
                startPos = 3;
                bytesToCopy = argument[2];
                break;
            case ORDEREDLIST:
            case UNORDEREDLIST:
            case UNION:
            case RECORD:
                startPos = 0;
                bytesToCopy = argument.length;
                break;
        }
        System.arraycopy(argument, startPos, reusableBuffers[index].array(), 0, bytesToCopy);
        reusableBuffers[index].position(0);
        reusableBuffers[index].limit(bytesToCopy);
    }

    @Override
    public ByteBuffer[] getArguments() {
        return reusableBuffers;
    }

}