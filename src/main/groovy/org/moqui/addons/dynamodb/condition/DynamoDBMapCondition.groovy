package org.moqui.impl.entity.dynamodb.condition

import org.moqui.impl.entity.dynamodb.DynamoDBEntityConditionFactoryImpl
import org.moqui.impl.entity.EntityDefinition
import org.moqui.entity.EntityCondition
import org.moqui.impl.entity.dynamodb.condition.DynamoDBEntityConditionImplBase
import org.moqui.impl.entity.dynamodb.DynamoDBUtils

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.Condition
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition

class DynamoDBMapCondition extends DynamoDBEntityConditionImplBase {

    protected final static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DynamoDBMapCondition.class)
    
    protected Class internalClass = null
    protected Map<String, ?> fieldMap
    protected EntityCondition.ComparisonOperator comparisonOperator
    protected EntityCondition.JoinOperator joinOperator
    protected boolean ignoreCase = false

    DynamoDBMapCondition(DynamoDBEntityConditionFactoryImpl ecFactoryImpl,
            Map<String, ?> fieldMap, EntityCondition.ComparisonOperator comparisonOperator,
            EntityCondition.JoinOperator joinOperator) {
        super(ecFactoryImpl)
        this.fieldMap = fieldMap ? fieldMap : new HashMap()
        this.comparisonOperator = comparisonOperator ? comparisonOperator : EntityCondition.EQUALS
        this.joinOperator = joinOperator ? joinOperator : EntityCondition.JoinOperator.AND
    }

    protected DynamoDBEntityConditionImplBase makeCondition() {
        List conditionList = new LinkedList()
        for (Map.Entry<String, ?> fieldEntry in this.fieldMap.entrySet()) {
            DynamoDBEntityConditionImplBase newCondition = (DynamoDBEntityConditionImplBase) this.ecFactoryImpl.makeCondition(fieldEntry.getKey(),
                    this.comparisonOperator, fieldEntry.getValue())
            if (this.ignoreCase) newCondition.ignoreCase()
            conditionList.add(newCondition)
        }
        return (DynamoDBEntityConditionImplBase) this.ecFactoryImpl.makeCondition(conditionList, this.joinOperator)
    }

    boolean mapMatches(Map<String, ?> map) { return null }
    EntityCondition ignoreCase() { return null }


    AttributeValue getDynamoDBHashValue(EntityDefinition ed) {
        Map<String, Object>priKeys = ed.getPrimaryKeys(fieldMap)
        AttributeValue retVal = null
        logger.info("DynamoDBMapCondition(48), priKeys: ${priKeys}, ed: ${ed}")
        for(Map.Entry fieldEntry in priKeys) {
        logger.info("DynamoDBMapCondition(48), fieldEntry: ${fieldEntry}")
            retVal = DynamoDBUtils.getAttributeValue(fieldEntry.key, priKeys, ed)
            break
        }
        return retVal;
    }

    AttributeValue getDynamoDBRangeValue(EntityDefinition ed) {
        List<Node> fieldNodes = ed.getFieldNodes(false, true, false)
        String indexName, fieldName
        AttributeValue retVal = null
            for (Node nd in fieldNodes) {
                indexName = nd."@index"
                if (nd."@is-range" == "true") {
                    fieldName = nd."@name"
        logger.info("DynamoDBMapCondition(64), indexName: ${indexName},fieldName: ${fieldName}, ${fieldMap}")
                    retVal =  DynamoDBUtils.getAttributeValue(fieldName, fieldMap, ed)
        logger.info("DynamoDBMapCondition(66), retVal: ${retVal}")
                    break;
                }
            }
        return retVal
    }


    Condition getDynamoDBCondition(EntityDefinition ed) {
        List<Node> fieldNodes = ed.getFieldNodes(false, true, false)
        String indexName, fieldName
        Condition retVal = null
        com.amazonaws.services.dynamodbv2.model.ComparisonOperator compOp = null
        AttributeValue attrVal = null
            for (Node nd in fieldNodes) {
                if (nd."@is-range" == "true") {
                    fieldName = nd."@name"
        logger.info("DynamoDBMapCondition(64), indexName: ${indexName},fieldName: ${fieldName}, ${fieldMap}")
                    //TODO: check that compare op is "EQUAL"
                    if (this.fieldMap[fieldName]) {
                        attrVal =  DynamoDBUtils.getAttributeValue(fieldName, fieldMap, ed)
        logger.info("DynamoDBMapCondition(66), attrVal: ${attrVal}")
                        retVal = new Condition()
                        retVal.setAttributeValueList([attrVal])
                        retVal.setComparisonOperator(ComparisonOperator.EQ)
                        break;
                    }
                }
            }
        return retVal
    }

    RangeKeyCondition getRangeCondition(EntityDefinition ed) {
        RangeKeyCondition rangeCond = null
        return rangeCond
    }

    Map <String, Condition> getDynamoDBScanConditionMap() {
        return null;
    }
    
    String toString() {
        return this.fieldMap
    }
}
