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

package edu.uci.ics.asterix.om.typecomputer.impl;

import java.util.ArrayList;
import java.util.List;

import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.hyracks.algebricks.common.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.common.exceptions.NotImplementedException;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;

/**
 * @author Xiaoyu Ma
 *         This class is a type computer for single string as input and returns integer or null
 *         NULL and String are acceptable argument type and Union as optional string is also ok.
 */
public class UnaryStringInt32OrNullTypeComputer implements IResultTypeComputer {

    public static final UnaryStringInt32OrNullTypeComputer INSTANCE = new UnaryStringInt32OrNullTypeComputer();

    private static final String errWrongTypeMsg = "Expects String Type.";
    private static final String errWrongArgN = "Wrong Argument Number.";

    private UnaryStringInt32OrNullTypeComputer() {
    }

    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        if (fce.getArguments().isEmpty())
            throw new AlgebricksException(errWrongArgN);
        ILogicalExpression arg0 = fce.getArguments().get(0).getValue();
        IAType t0;
        try {
            t0 = (IAType) env.getType(arg0);
        } catch (AlgebricksException e) {
            throw new AlgebricksException(e);
        }
        if (t0 instanceof AUnionType) {
            AUnionType union = (AUnionType) t0;
            if (!((union.getUnionList().get(0).equals(BuiltinType.ASTRING) && union.getUnionList().get(1)
                    .equals(BuiltinType.ANULL)) || (union.getUnionList().get(1).equals(BuiltinType.ASTRING) && union
                    .getUnionList().get(0).equals(BuiltinType.ANULL))))
                throw new AlgebricksException(errWrongTypeMsg);
        } else if (t0.getTypeTag() != ATypeTag.NULL && t0.getTypeTag() != ATypeTag.STRING) {
            throw new NotImplementedException("Expects String Type.");
        }

        List<IAType> unionList = new ArrayList<IAType>();
        unionList.add(BuiltinType.ANULL);
        if (t0.getTypeTag() == ATypeTag.NULL) {
            return BuiltinType.ANULL;
        }

        if (t0.getTypeTag() == ATypeTag.STRING) {
            unionList.add(BuiltinType.AINT32);
        }

        return new AUnionType(unionList, "String-length-Result");
    }
}
