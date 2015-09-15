/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.asterix.om.pointables.nonvisitor;

import org.apache.asterix.om.util.container.IObjectFactory;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.data.std.api.AbstractPointable;
import org.apache.hyracks.data.std.api.IPointable;
import org.apache.hyracks.data.std.api.IPointableFactory;
import org.apache.hyracks.data.std.primitive.BytePointable;
import org.apache.hyracks.data.std.primitive.LongPointable;

/*
 * This class interprets the binary data representation of an interval.
 *
 * The interval can be time, date, or datetime defined by the tag.
 *
 * Interval {
 *   int startPoint;
 *   int endPoint;
 *   byte tag;
 * }
 */
public class AIntervalPointable extends AbstractPointable {

    public static final ITypeTraits TYPE_TRAITS = new ITypeTraits() {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean isFixedLength() {
            return true;
        }

        @Override
        public int getFixedLength() {
            return 17;
        }
    };

    public static final IPointableFactory FACTORY = new IPointableFactory() {
        private static final long serialVersionUID = 1L;

        @Override
        public IPointable createPointable() {
            return new AIntervalPointable();
        }

        @Override
        public ITypeTraits getTypeTraits() {
            return TYPE_TRAITS;
        }
    };

    public static final IObjectFactory<IPointable, String> ALLOCATOR = new IObjectFactory<IPointable, String>() {
        public IPointable create(String id) {
            return new AIntervalPointable();
        }
    };

    private static final int TAG_SIZE = 1;
    private static final int START_LENGTH_SIZE = 8;
    private static final int END_LENGTH_SIZE = 8;

    public long getStart() {
        return LongPointable.getLong(bytes, getStartOffset());
    }

    public int getStartOffset() {
        return start;
    }

    public int getStartSize() {
        return START_LENGTH_SIZE;
    }

    public long getEnd() {
        return LongPointable.getLong(bytes, getEndOffset());
    }

    public int getEndOffset() {
        return getStartOffset() + getStartSize();
    }

    public int getEndSize() {
        return END_LENGTH_SIZE;
    }

    public byte getTag() {
        return BytePointable.getByte(bytes, getTagOffset());
    }

    public int getTagOffset() {
        return getEndOffset() + getEndSize();
    }

    public int getTagSize() {
        return TAG_SIZE;
    }

}
