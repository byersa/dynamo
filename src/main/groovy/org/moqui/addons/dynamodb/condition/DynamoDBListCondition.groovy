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

class DynamoDBListCondition extends DynamoDBEntityConditionImplBase {
    protected final static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DynamoDBListCondition.class)
    protected Class localClass = null
    protected List<DynamoDBEntityConditionImplBase> conditionList
    protected EntityCondition.JoinOperator operator

    DynamoDBListCondition(DynamoDBEntityConditionFactoryImpl ecFactoryImpl,
            List<DynamoDBEntityConditionImplBase> conditionList, EntityCondition.JoinOperator operator) {
        super(ecFactoryImpl)
        this.conditionList = conditionList ?: new LinkedList()
        this.operator = operator ?: EntityCondition.JoinOperator.AND

    }

    void addCondition(DynamoDBEntityConditionImplBase condition) { conditionList.add(condition) }

    boolean mapMatches(Map<String, ?> map) { return null }
    EntityCondition ignoreCase() { return null }


    AttributeValue getDynamoDBHashValue(EntityDefinition ed) {
        AttributeValue attrVal = null
        for(DynamoDBEntityConditionImplBase cond in conditionList) {
            attrVal = cond.getDynamoDBHashValue(ed)
            if (attrVal) {
                break
            }
        }
        return attrVal
    }

    AttributeValue getDynamoDBRangeValue(EntityDefinition ed) {
        List<Node> fieldNodes = ed.getFieldNodes(false, true, false)
        String indexName, indexFieldName
        for (Node nd in fieldNodes) {
            indexName = nd."@index"
            if (indexName) {
                indexFieldName = nd."@name"
                break
            }
        }
        if (!indexFieldName) {
            return null
        }
        AttributeValue attrVal = null
        for(DynamoDBEntityConditionImplBase cond in conditionList) {
            attrVal = cond.getDynamoDBHashValue(ed)
            if (attrVal) {
                break
            }
        }
        return attrVal
    }

    Condition getDynamoDBRangeCondition(EntityDefinition ed) {
        List<Node> fieldNodes = ed.getFieldNodes(false, true, false)
        String indexName, indexFieldName
        List<ComparisonOperator> rangeOperators = new LinkedList()
        List<AttributeValue> attributeValues = new LinkedList()
        for (Node nd in fieldNodes) {
            indexName = nd."@index"
            if (indexName) {
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
}
