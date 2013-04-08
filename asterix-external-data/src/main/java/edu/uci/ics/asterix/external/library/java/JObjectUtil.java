package edu.uci.ics.asterix.external.library.java;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.common.exceptions.AsterixException;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.AStringSerializerDeserializer;
import edu.uci.ics.asterix.dataflow.data.nontagged.serde.SerializerDeserializerUtil;
import edu.uci.ics.asterix.external.library.java.JObjects.ByteArrayAccessibleDataInputStream;
import edu.uci.ics.asterix.external.library.java.JObjects.JBoolean;
import edu.uci.ics.asterix.external.library.java.JObjects.JCircle;
import edu.uci.ics.asterix.external.library.java.JObjects.JDate;
import edu.uci.ics.asterix.external.library.java.JObjects.JDateTime;
import edu.uci.ics.asterix.external.library.java.JObjects.JDouble;
import edu.uci.ics.asterix.external.library.java.JObjects.JDuration;
import edu.uci.ics.asterix.external.library.java.JObjects.JFloat;
import edu.uci.ics.asterix.external.library.java.JObjects.JInt;
import edu.uci.ics.asterix.external.library.java.JObjects.JInterval;
import edu.uci.ics.asterix.external.library.java.JObjects.JLine;
import edu.uci.ics.asterix.external.library.java.JObjects.JOrderedList;
import edu.uci.ics.asterix.external.library.java.JObjects.JPoint;
import edu.uci.ics.asterix.external.library.java.JObjects.JPoint3D;
import edu.uci.ics.asterix.external.library.java.JObjects.JPolygon;
import edu.uci.ics.asterix.external.library.java.JObjects.JRecord;
import edu.uci.ics.asterix.external.library.java.JObjects.JRectangle;
import edu.uci.ics.asterix.external.library.java.JObjects.JString;
import edu.uci.ics.asterix.external.library.java.JObjects.JTime;
import edu.uci.ics.asterix.external.library.java.JObjects.JUnorderedList;
import edu.uci.ics.asterix.om.types.AOrderedListType;
import edu.uci.ics.asterix.om.types.ARecordType;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnorderedListType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.EnumDeserializer;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.util.NonTaggedFormatUtil;
import edu.uci.ics.asterix.om.util.container.IObjectPool;

public class JObjectUtil {

    public static IJObject getJType(ATypeTag typeTag, IAType type, ByteArrayAccessibleDataInputStream dis,
            IObjectPool<IJObject, IAType> objectPool) throws IOException, AsterixException {
        IJObject jObject;
       

        switch (typeTag) {

            case INT32: {
                int v = dis.readInt();
                jObject = objectPool.allocate(BuiltinType.AINT32);
                ((JInt) jObject).setValue(v);
                break;
            }

            case FLOAT: {
                float v = dis.readFloat();
                jObject = objectPool.allocate(BuiltinType.AFLOAT);
                ((JFloat) jObject).setValue(v);
                break;
            }

            case DOUBLE: {
                double value = dis.readDouble();
                jObject = objectPool.allocate(BuiltinType.ADOUBLE);
                ((JDouble) jObject).setValue(value);
                break;
            }

            case STRING: {
                String v = dis.readUTF();
                jObject = objectPool.allocate(BuiltinType.ASTRING);
                ((JString) jObject).setValue(v);
                break;
            }

            case BOOLEAN:
                jObject = objectPool.allocate(BuiltinType.ABOOLEAN);
                ((JBoolean) jObject).setValue(dis.readBoolean());
                break;

            case DATE: {
                int d = dis.readInt();
                jObject = objectPool.allocate(BuiltinType.ADATE);
                ((JDate) jObject).setValue(d);
                break;
            }

            case DATETIME: {
                jObject = objectPool.allocate(BuiltinType.ADATETIME);
                long value = dis.readLong();
                ((JDateTime) jObject).setValue(value);
                break;
            }

            case DURATION: {
                jObject = objectPool.allocate(BuiltinType.ADURATION);
                int months = dis.readInt();
                long msecs = dis.readLong();
                ((JDuration) jObject).setValue(months, msecs);
                break;
            }

            case TIME: {
                jObject = objectPool.allocate(BuiltinType.ATIME);
                int time = dis.readInt();
                ((JTime) jObject).setValue(time);
                break;
            }

            case INTERVAL: {
                jObject = objectPool.allocate(BuiltinType.AINTERVAL);
                long start = dis.readLong();
                long end = dis.readLong();
                byte intervalType = dis.readByte();
                ((JInterval) jObject).setValue(start, end, intervalType);
                break;
            }

            case CIRCLE: {
                jObject = objectPool.allocate(BuiltinType.ACIRCLE);
                double x = dis.readDouble();
                double y = dis.readDouble();
                double radius = dis.readDouble();
                JPoint jpoint = (JPoint) objectPool.allocate(BuiltinType.APOINT);
                jpoint.setValue(x, y);
                ((JCircle) jObject).setValue(jpoint, radius);
                break;
            }

            case POINT: {
                jObject = objectPool.allocate(BuiltinType.APOINT);
                double x = dis.readDouble();
                double y = dis.readDouble();
                ((JPoint) jObject).setValue(x, y);
                break;
            }

            case POINT3D: {
                jObject = objectPool.allocate(BuiltinType.APOINT3D);
                double x = dis.readDouble();
                double y = dis.readDouble();
                double z = dis.readDouble();
                ((JPoint3D) jObject).setValue(x, y, z);
                break;
            }

            case LINE: {
                jObject = objectPool.allocate(BuiltinType.ALINE);
                double x1 = dis.readDouble();
                double y1 = dis.readDouble();
                double x2 = dis.readDouble();
                double y2 = dis.readDouble();
                JPoint jpoint1 = (JPoint) objectPool.allocate(BuiltinType.APOINT);
                jpoint1.setValue(x1, y1);
                JPoint jpoint2 = (JPoint) objectPool.allocate(BuiltinType.APOINT);
                jpoint2.setValue(x2, y2);
                ((JLine) jObject).setValue(jpoint1, jpoint2);
                break;
            }

            case POLYGON: {
                jObject = objectPool.allocate(BuiltinType.APOLYGON);
                short numberOfPoints = dis.readShort();
                List<JPoint> points = new ArrayList<JPoint>();
                for (int i = 0; i < numberOfPoints; i++) {
                    JPoint p1 = (JPoint) objectPool.allocate(BuiltinType.APOINT);
                    p1.setValue(dis.readDouble(), dis.readDouble());
                    points.add(p1);
                }
                ((JPolygon) jObject).setValue(points);
                break;
            }

            case RECTANGLE: {
                jObject = objectPool.allocate(BuiltinType.ARECTANGLE);
                double x1 = dis.readDouble();
                double y1 = dis.readDouble();
                double x2 = dis.readDouble();
                double y2 = dis.readDouble();
                JPoint jpoint1 = (JPoint) objectPool.allocate(BuiltinType.APOINT);
                jpoint1.setValue(x1, y1);
                JPoint jpoint2 = (JPoint) objectPool.allocate(BuiltinType.APOINT);
                jpoint2.setValue(x2, y2);
                ((JRectangle) jObject).setValue(jpoint1, jpoint2);
                break;
            }

            case UNORDEREDLIST: {
                AUnorderedListType listType = (AUnorderedListType) type;
                IAType elementType = listType.getItemType();
                jObject = objectPool.allocate(listType);

                boolean fixedSize = false;
                ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(dis.readByte());
                switch (tag) {
                    case STRING:
                    case RECORD:
                    case ORDEREDLIST:
                    case UNORDEREDLIST:
                    case ANY:
                        fixedSize = false;
                        break;
                    default:
                        fixedSize = true;
                        break;
                }
                dis.readInt(); // list size
                int numberOfitems;
                numberOfitems = dis.readInt();
                if (numberOfitems > 0) {
                    if (!fixedSize) {
                        for (int i = 0; i < numberOfitems; i++)
                            dis.readInt();
                    }
                    for (int i = 0; i < numberOfitems; i++) {
                        IJObject v = (IJObject) getJType(elementType.getTypeTag(), elementType, dis, objectPool);
                        ((JUnorderedList) jObject).add(v);
                    }
                }

                break;
            }
            case ORDEREDLIST: {
                AOrderedListType listType = (AOrderedListType) type;
                IAType elementType = listType.getItemType();
                jObject = objectPool.allocate(listType);
                boolean fixedSize = false;
                ATypeTag tag = EnumDeserializer.ATYPETAGDESERIALIZER.deserialize(dis.readByte());
                switch (tag) {
                    case STRING:
                    case RECORD:
                    case ORDEREDLIST:
                    case UNORDEREDLIST:
                    case ANY:
                        fixedSize = false;
                        break;
                    default:
                        fixedSize = true;
                        break;
                }

                dis.readInt(); // list size
                int numberOfitems;
                numberOfitems = dis.readInt();
                if (numberOfitems > 0) {
                    if (!fixedSize) {
                        for (int i = 0; i < numberOfitems; i++)
                            dis.readInt();
                    }
                    for (int i = 0; i < numberOfitems; i++) {
                        IJObject v = (IJObject) getJType(elementType.getTypeTag(), elementType, dis, objectPool);
                        ((JOrderedList) jObject).add(v);
                    }
                }

                break;
            }
            case RECORD:
                ARecordType recordType = (ARecordType) type;
                boolean isExpanded = false;
                int v1 = dis.readInt(); // recordSize
                if (recordType == null) {
                    isExpanded = dis.readBoolean();
                    int v2 = dis.readInt(); // openPartOffset
                } else {
                    if (recordType.isOpen()) {
                        isExpanded = dis.readBoolean();
                        if (isExpanded)
                            dis.readInt(); // openPartOffset
                    } else
                        isExpanded = false;
                }
                int numberOfSchemaFields = recordType.getFieldNames().length;
                IJObject[] closedFields = new IJObject[numberOfSchemaFields];
                if (numberOfSchemaFields > 0) {
                    dis.readInt(); // read number of closed fields.
                    boolean hasNullableFields = NonTaggedFormatUtil.hasNullableField(recordType);
                    byte[] nullBitMap = null;
                    if (hasNullableFields) {
                        int nullBitMapSize = (int) (Math.ceil(numberOfSchemaFields / 8.0));
                        nullBitMap = new byte[nullBitMapSize];
                        dis.readFully(nullBitMap);
                    }
                    for (int i = 0; i < numberOfSchemaFields; i++) {
                        int v3 = dis.readInt();
                    }
                    for (int fieldId = 0; fieldId < numberOfSchemaFields; fieldId++) {
                        if (hasNullableFields && ((nullBitMap[fieldId / 8] & (1 << (7 - (fieldId % 8)))) == 0)) {
                            closedFields[fieldId] = null;
                            continue;
                        }
                        IAType elementType = recordType.getFieldTypes()[fieldId];
                        closedFields[fieldId] = getJType(elementType.getTypeTag(), elementType, dis, objectPool);
                    }
                }

                if (isExpanded) {
                    int numberOfOpenFields = dis.readInt();
                    String[] fieldNames = new String[numberOfOpenFields];
                    IAType[] fieldTypes = new IAType[numberOfOpenFields];
                    IJObject[] openFields = new IJObject[numberOfOpenFields];
                    for (int i = 0; i < numberOfOpenFields; i++) {
                        dis.readInt();
                        dis.readInt();
                    }
                    for (int i = 0; i < numberOfOpenFields; i++) {
                        fieldNames[i] = AStringSerializerDeserializer.INSTANCE.deserialize(dis).getStringValue();
                        ATypeTag openFieldTypeTag = SerializerDeserializerUtil.deserializeTag(dis);
                        openFields[i] = getJType(openFieldTypeTag, null, dis, objectPool);
                        fieldTypes[i] = openFields[i].getIAObject().getType();
                    }
                    ARecordType openPartRecType = new ARecordType(null, fieldNames, fieldTypes, true);
                    if (numberOfSchemaFields > 0) {
                        ARecordType mergedRecordType = mergeRecordTypes(recordType, openPartRecType);
                        IJObject[] mergedFields = mergeFields(closedFields, openFields);
                        jObject = objectPool.allocate(recordType);
                        return new JRecord(mergedRecordType, mergedFields);
                    } else {
                        return new JRecord(recordType, openFields);
                    }
                } else {
                    return new JRecord(recordType, closedFields);
                }

            default:
                throw new IllegalStateException("Argument type: " + typeTag);
        }
        return jObject;
    }

    private static IJObject[] mergeFields(IJObject[] closedFields, IJObject[] openFields) {
        IJObject[] fields = new IJObject[closedFields.length + openFields.length];
        int i = 0;
        for (; i < closedFields.length; i++) {
            fields[i] = closedFields[i];
        }
        for (int j = 0; j < openFields.length; j++) {
            fields[closedFields.length + j] = openFields[j];
        }
        return fields;
    }

    private static ARecordType mergeRecordTypes(ARecordType recType1, ARecordType recType2) throws AsterixException {

        String[] fieldNames = new String[recType1.getFieldNames().length + recType2.getFieldNames().length];
        IAType[] fieldTypes = new IAType[recType1.getFieldTypes().length + recType2.getFieldTypes().length];

        int i = 0;
        for (; i < recType1.getFieldNames().length; i++) {
            fieldNames[i] = recType1.getFieldNames()[i];
            fieldTypes[i] = recType1.getFieldTypes()[i];
        }

        for (int j = 0; j < recType2.getFieldNames().length; i++, j++) {
            fieldNames[i] = recType2.getFieldNames()[j];
            fieldTypes[i] = recType2.getFieldTypes()[j];
        }
        return new ARecordType(null, fieldNames, fieldTypes, true);
    }
}
