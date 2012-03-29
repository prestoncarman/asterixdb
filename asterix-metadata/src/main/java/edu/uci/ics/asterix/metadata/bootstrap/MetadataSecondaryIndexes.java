/*
 * Copyright 2009-2010 by The Regents of the University of California
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

package edu.uci.ics.asterix.metadata.bootstrap;

import edu.uci.ics.asterix.metadata.MetadataException;
import edu.uci.ics.asterix.metadata.api.IMetadataIndex;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;

/**
 * Contains static secondary-index descriptors on metadata datasets.
 */
public class MetadataSecondaryIndexes {
    public static IMetadataIndex GROUPNAME_ON_DATASET_INDEX;
    public static IMetadataIndex DATATYPENAME_ON_DATASET_INDEX;
    public static IMetadataIndex DATATYPENAME_ON_DATATYPE_INDEX;

    //The following resourceIds' value must be monotonically increasing value and
    //the value of GROUPNAME_ON_DATASET_INDEX_RESOURCE_ID must be greater by one than 
    //FUNCTION_DATASET_RESOURCE_ID which is 6 (from MetadataPrimaryIndexes.java)
    //Instead of creating the value using the resourceIdSeed member variable of MetadataNode,
    //the value is assigned a static value.
    //Also, the MetadataSecondaryIndexes' resourceId is generated in the same manner.
    //(Please see the MetadataPrimaryIndexes.java.)
    //Therefore, the resourceIdSeed must be set to the MetadataSecondaryIndexes' the last resourceId + 1.
    public static final int GROUPNAME_ON_DATASET_INDEX_RESOURCE_ID = 8;
    public static final int DATATYPENAME_ON_DATASET_INDEX_RESOURCE_ID = 9;
    public static final int DATATYPENAME_ON_DATATYPE_INDEX_RESOURCE_ID = 10;
    
    /**
     * Create all metadata secondary index descriptors. MetadataRecordTypes must
     * have been initialized before calling this init.
     * 
     * @throws MetadataException
     *             If MetadataRecordTypes have not been initialized.
     */
    public static void init() throws MetadataException {
        // Make sure the MetadataRecordTypes have been initialized.
        if (MetadataRecordTypes.DATASET_RECORDTYPE == null) {
            throw new MetadataException(
                    "Must initialize MetadataRecordTypes before initializing MetadataSecondaryIndexes.");
        }

        GROUPNAME_ON_DATASET_INDEX = new MetadataIndex("Dataset", "GroupName", 3, new IAType[] { BuiltinType.ASTRING,
                BuiltinType.ASTRING, BuiltinType.ASTRING },
                new String[] { "GroupName", "DataverseName", "DatasetName" }, null, GROUPNAME_ON_DATASET_INDEX_RESOURCE_ID);

        DATATYPENAME_ON_DATASET_INDEX = new MetadataIndex("Dataset", "DatatypeName", 3, new IAType[] {
                BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING }, new String[] { "DataverseName",
                "DatatypeName", "DatasetName" }, null, DATATYPENAME_ON_DATASET_INDEX_RESOURCE_ID);

        DATATYPENAME_ON_DATATYPE_INDEX = new MetadataIndex("Datatype", "DatatypeName", 3, new IAType[] {
                BuiltinType.ASTRING, BuiltinType.ASTRING, BuiltinType.ASTRING }, new String[] { "DataverseName",
                "NestedDatatypeName", "TopDatatypeName" }, null, DATATYPENAME_ON_DATATYPE_INDEX_RESOURCE_ID);
    }
}