/*
 * This Work is in the public domain and is provided on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied,
 * including, without limitation, any warranties or conditions of TITLE,
 * NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A PARTICULAR PURPOSE.
 * You are solely responsible for determining the appropriateness of using
 * this Work and assume any risks associated with your use of this Work.
 *
 * This Work includes contributions authored by David E. Jones, not as a
 * "work for hire", who hereby disclaims any copyright to the same.
 */
package org.moqui.impl.entity.dynamodb.condition

import org.moqui.impl.entity.EntityConditionFactoryImpl
import org.moqui.impl.entity.dynamodb.DynamoDBEntityConditionFactoryImpl
import org.moqui.impl.entity.EntityQueryBuilder
import org.moqui.impl.entity.EntityDefinition
import org.moqui.entity.EntityCondition
import org.moqui.impl.entity.condition.ListCondition
import org.moqui.impl.entity.dynamodb.condition.DynamoDBEntityConditionImplBase

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator
import com.amazonaws.services.dynamodbv2.model.Condition

import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.moqui.util.MNode 

class DynamoDBListCondition extends DynamoDBEntityConditionImplBase {
    protected final static Logger logger = LoggerFactory.getLogger(DynamoDBListCondition.class)
    protected Class localClass = null
    protected List<DynamoDBEntityConditionImplBase> conditionList
    protected EntityCondition.JoinOperator operator

    DynamoDBListCondition(DynamoDBEntityConditionFactoryImpl ecFactoryImpl,
            List<DynamoDBEntityConditionImplBase> conditionList, EntityCondition.JoinOperator operator) {
        super(ecFactoryImpl)
        this.conditionList = conditionList ?: new LinkedList()
        this.operator = operator ?: EntityCondition.JoinOperator.AND

    }

    void addCondition(DynamoDBEntityConditionImplBase condition) { 
         conditionList.add(condition) 
                   logger.info("DynamoDBListCondition, conditionList: ${conditionList}")
    }

    boolean mapMatches(Map<String, ?> map) { return null }
    EntityCondition ignoreCase() { return null }


    String getDynamoDBHashValue(EntityDefinition ed) {
        String val
        for(DynamoDBEntityConditionImplBase cond in conditionList) {
            if (!val) {
                val = cond.getDynamoDBHashValue(ed)
            }
        }
        return val
    }

    Map <String, String> getDynamoDBIndexValue(EntityDefinition ed) {
        Map <String, String> val
        for(DynamoDBEntityConditionImplBase cond in conditionList) {
            if (!val) {
                val = cond.getDynamoDBIndexValue(ed)
            }
        }
        return val
    }


    String getDynamoDBRangeValue(EntityDefinition ed) {
        String val
        for(DynamoDBEntityConditionImplBase cond in conditionList) {
            if (!val) {
                val = cond.getDynamoDBRangeValue(ed)
            }
        }
        return val
    }

    RangeKeyCondition getRangeCondition(EntityDefinition ed) {
        RangeKeyCondition rangeCondition
        for(DynamoDBEntityConditionImplBase cond in conditionList) {
                   logger.info("DynamoDBListCondition, getRangeCondition, cond: ${cond}")
                   logger.info("DynamoDBListCondition, getRangeCondition, cond.class: ${cond.class}")
            if (!rangeCondition) {
                rangeCondition = cond.getRangeCondition(ed)
                   logger.info("DynamoDBListCondition, getRangeCondition, rangeCondition: ${rangeCondition}")
            }
        }
        return rangeCondition
    }

    Map getDynamoDBFilterExpressionMap(EntityDefinition ed, List skipFieldNames) {
        Map retMap, tmpMap
        for(DynamoDBEntityConditionImplBase cond in conditionList) {
            tmpMap = cond.getDynamoDBFilterExpressionMap(ed, skipFieldNames)
            if (tmpMap) {
                   logger.info("DynamoDBListCondition, tmpMap: ${tmpMap}")
                if (!retMap) {
                    retMap = new HashMap()
                    retMap["filterExpression"] = tmpMap.filterExpression
                    retMap["nameMap"] = tmpMap.nameMap
                    retMap["valueMap"] = tmpMap.valueMap
                } else {
                    retMap.filterExpression += " AND " + tmpMap.filterExpression
                    retMap.nameMap.putAll(tmpMap.nameMap)
                    retMap.valueMap.putAll(tmpMap.valueMap)
                }
            }
        }
        logger.info("DynamoDBListCondition, retMap: ${retMap}")
        return retMap
    }

    Condition getDynamoDBCondition(EntityDefinition ed) {
        List<MNode> fieldNodes = ed.getFieldNodes(false, true, false)
        String indexName, indexFieldName
        List<ComparisonOperator> rangeOperators = new LinkedList()
        List<AttributeValue> attributeValues = new LinkedList()
        for (MNode nd in fieldNodes) {
            indexName = nd."@index"
            if (nd."@is_range") {
                indexFieldName = nd."@name"
                break
            }
        }
        if (!indexFieldName) {
            return null
        }
        AttributeValue attrVal = null
        Condition rangeCond = null 
        String compOp = null
        for(DynamoDBEntityConditionImplBase cond in conditionList) {
            rangeCond = cond.getDynamoDBRangeCondition(ed)
            if (rangeCond) {
                compOp = rangeCond.getComparisonOperator()
                rangeOperators.add(ComparisonOperator.fromValue(compOp))
                attributeValues.addAll(rangeCond.getAttributeValueList())
            }
        }
        
        logger.info("DynamoDBListCondition, getDynamoDBRangeCondition(107), attributeValues: ${attributeValues}, rangeOperators: ${rangeOperators}")
        //  analyze range values
        // if two with GT and LT, then use BETWEEN
        // if all have EQ op, then make a contains (if multiple) 
        
        // if one value then return it as randEQ if just one value
        if (attributeValues.size == 0) {
            return null
        }
        Condition returnCond = new Condition()
        if (attributeValues.size == 1) {
            returnCond.setAttributeValueList(attributeValues)
            returnCond.setComparisonOperator(rangeOperators[0])
        } else if (attributeValues.size == 2 
                   && ComparisonOperator.GE in rangeOperators
                   && ComparisonOperator.LE in rangeOperators ) {
            returnCond.setComparisonOperator(ComparisonOperator.BETWEEN)
            returnCond.setAttributeValueList(attributeValues.sort{a,b-> a.getS() <=> b.getS()})
        } else if (attributeValues.size > 1 && rangeOperators.every{it == ComparisonOperator.EQ} ) {
            returnCond.setComparisonOperator(ComparisonOperator.IN)
            returnCond.setAttributeValueList(attributeValues)
        }
        
        
        return returnCond
    }

    Map <String, Condition> getDynamoDBScanConditionMap() {
        return null;
    }
    
    String toString() {
        return this.conditionList
    }

    // Dummied out calls
    void readExternal(java.io.ObjectInput obj) { return }
    boolean mapMatchesAny(java.util.Map obj) { return null }
    void writeExternal(java.io.ObjectOutput obj) { return }

}
