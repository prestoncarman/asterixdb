package edu.uci.ics.asterix.om.typecomputer.impl;


import edu.uci.ics.asterix.om.typecomputer.base.IResultTypeComputer;
import edu.uci.ics.asterix.om.types.ATypeTag;
import edu.uci.ics.asterix.om.types.AUnionType;
import edu.uci.ics.asterix.om.types.BuiltinType;
import edu.uci.ics.asterix.om.types.IAType;
import edu.uci.ics.asterix.om.types.TypeHelper;
import edu.uci.ics.hyracks.algebricks.core.algebra.base.ILogicalExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.AbstractFunctionCallExpression;
import edu.uci.ics.hyracks.algebricks.core.algebra.expressions.IVariableTypeEnvironment;
import edu.uci.ics.hyracks.algebricks.core.algebra.metadata.IMetadataProvider;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.AlgebricksException;
import edu.uci.ics.hyracks.algebricks.core.api.exceptions.NotImplementedException;

/**
 *
 * @author Xiaoyu Ma
 */
public class UnaryStringInt32OrNullTypeComputer implements IResultTypeComputer  {   
    
    public static final UnaryStringInt32OrNullTypeComputer INSTANCE = new UnaryStringInt32OrNullTypeComputer();
    private UnaryStringInt32OrNullTypeComputer() {}
    
    @Override
    public IAType computeType(ILogicalExpression expression, IVariableTypeEnvironment env,
            IMetadataProvider<?, ?> metadataProvider) throws AlgebricksException {
        AbstractFunctionCallExpression fce = (AbstractFunctionCallExpression) expression;
        ILogicalExpression arg0 = fce.getArguments().get(0).getValue();
        IAType t0;
        try {
            t0 = (IAType) env.getType(arg0);
        } catch (AlgebricksException e) {
            throw new AlgebricksException(e);
        }
        if (t0.getTypeTag() != ATypeTag.NULL &&
            t0.getTypeTag() != ATypeTag.STRING ) {
            throw new NotImplementedException("Expects String Type.");
        }
        
        if (TypeHelper.canBeNull(t0)) {
            return AUnionType.createNullableType(BuiltinType.AINT32);
        }        

        return BuiltinType.AINT32;
    }      
}
