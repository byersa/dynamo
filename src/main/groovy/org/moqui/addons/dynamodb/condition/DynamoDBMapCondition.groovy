package org.moqui.impl.entity.dynamodb.condition

import org.moqui.impl.entity.dynamodb.DynamoDBEntityConditionFactoryImpl
import org.moqui.impl.entity.EntityDefinition
import org.moqui.entity.EntityCondition
import org.moqui.entity.EntityCondition.ComparisonOperator
import org.moqui.entity.EntityCondition.JoinOperator
import org.moqui.impl.entity.dynamodb.condition.DynamoDBEntityConditionImplBase
import org.moqui.impl.entity.dynamodb.DynamoDBUtils

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.Condition
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.moqui.util.MNode 

class DynamoDBMapCondition extends DynamoDBEntityConditionImplBase {

    protected final static Logger logger = LoggerFactory.getLogger(DynamoDBMapCondition.class)
    
    protected Class internalClass = null
    protected Map<String, ?> fieldMap
    protected org.moqui.entity.EntityCondition.ComparisonOperator comparisonOperator
    protected org.moqui.entity.EntityCondition.JoinOperator joinOperator
    protected boolean ignoreCase = false

    DynamoDBMapCondition(DynamoDBEntityConditionFactoryImpl ecFactoryImpl,
            Map<String, ?> fieldMap, org.moqui.entity.EntityCondition.ComparisonOperator comparisonOperator,
            org.moqui.entity.EntityCondition.JoinOperator joinOperator) {
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


    String getDynamoDBHashValue(EntityDefinition ed) {
        Map<String, Object>priKeys = ed.getPrimaryKeys(fieldMap)
        String retVal = null
        logger.info("DynamoDBMapCondition(48), priKeys: ${priKeys}, ed: ${ed}")
        for(Map.Entry fieldEntry in priKeys) {
            logger.info("DynamoDBMapCondition(48), fieldEntry: ${fieldEntry}")
            if( this.fieldMap[fieldEntry.key]) {
                retVal = this.fieldMap[fieldEntry.key]
                break
            }
        }
        logger.info("DynamoDBMapCondition(1), retVal: ${retVal}")
        return retVal;
    }

    Map getDynamoDBFilterExpressionMap(EntityDefinition ed, List skipFieldNames) {
        Map retMap
        logger.info("in getDynamoDBFilterExpressionMap, skipFieldNames: ${skipFieldNames}")
        ArrayList <MNode> fieldNodes = ed.entityNode.getChildren()
        String indexName, fieldName, fieldValue
        String filterExpression = ""
        Map attrNameMap = new HashMap()
        Map attrValueMap = new HashMap()
        for (MNode nd in fieldNodes) {
            fieldName = nd.attribute("name")
            logger.info("in getDynamoDBFilterExpressionMap, fieldName: ${fieldName}")
            if (skipFieldNames.indexOf(fieldName) < 0) {
                if( this.fieldMap[fieldName]) {
                    if (filterExpression) { filterExpression += " AND " }
                    filterExpression += "#${fieldName} = :${fieldName} "
                    logger.info("in getDynamoDBFilterExpressionMap, filterExpression: ${filterExpression}")
                    attrNameMap.put("#" + fieldName, fieldName)
                    attrValueMap.put(":" + fieldName, this.fieldMap[fieldName])
                    break
                }
            }
        }
        if (filterExpression) {
            retMap = new HashMap()
            retMap["filterExpression"] = filterExpression
            retMap["nameMap"] = attrNameMap
            retMap["valueMap"] = attrValueMap
        }
                    logger.info("in getDynamoDBFilterExpressionMap, retMap: ${retMap}")
        return retMap
    }

    Map <String, String> getDynamoDBIndexValue(EntityDefinition ed) {
         
         Map <String, String> retVal
                        logger.info("DynamoDBMapCondition, getDynamoDBIndexValue, ed.entityNode: ${ed.entityNode}")
                for (MNode indexNode in ed.entityNode."index") {
                        logger.info("DynamoDBMapCondition, getDynamoDBIndexValue, indexNode: ${indexNode}")
                    String indexFieldName
                    for (MNode indexFieldNode in indexNode."index-field") {
                        indexFieldName = indexFieldNode."@name"
                        logger.info("DynamoDBMapCondition, getDynamoDBIndexValue, indexFieldName: ${indexFieldName}")
                        if( this.fieldMap[indexFieldName]) {
                            retVal = new HashMap()
                            // indexNode."@name" has the secondary index name that DynamoDB knows
                            retVal.put("indexName", indexNode."@name")
                            retVal.put("indexFieldName", indexFieldName)
                            retVal.put("indexFieldValue", this.fieldMap[indexFieldName])
                            break
                        }
                    }   
                }
        return retVal;
    }

    AttributeValue getDynamoDBHashAttributeValue(EntityDefinition ed) {
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

    String getDynamoDBRangeValue(EntityDefinition ed) {
        ArrayList <MNode> fieldNodes = ed.entityNode.getChildren()
        String indexName, fieldName, retVal = null
        for (MNode nd in fieldNodes) {
                if (nd.attribute("is-range") == "true") {
                    fieldName = nd.attribute("name")
        logger.info("DynamoDBMapCondition(139), fieldName: ${fieldName}, ${fieldMap}")
                    retVal =  fieldMap[fieldName]
        logger.info("DynamoDBMapCondition(141), retVal: ${retVal}")
                    break;
                }
        }
        return retVal
    }


    Condition getDynamoDBCondition(EntityDefinition ed) {
        ArrayList <MNode> fieldNodes = ed.entityNode.getChildren()
        String indexName, fieldName
        Condition retVal = null
        com.amazonaws.services.dynamodbv2.model.ComparisonOperator compOp = null
        AttributeValue attrVal = null
            for (MNode nd in fieldNodes) {
                if (nd.attribute("is-range") == "true") {
                    fieldName = nd.attribute("name")
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
        ArrayList <MNode> fieldNodes = ed.entityNode.getChildren()
        String indexName, fieldName
        RangeKeyCondition rangeCond = null
        String attrVal = null
            for (MNode nd in fieldNodes) {
                if (!rangeCond && nd.attribute("is-range") == "true") {
                    fieldName = nd.attribute("name")
                    logger.info("in getRangeCondition, fieldName: ${fieldName}, value: ${this.fieldMap[fieldName]}")
                    if (this.fieldMap[fieldName]) {
                        rangeCond = new RangeKeyCondition(fieldName).eq(this.fieldMap[fieldName])
                        logger.info("in getRangeCondition, rangeCond: ${rangeCond}")
                    }
                }
            }
        return rangeCond
    }

    Map <String, Condition> getDynamoDBScanConditionMap() {
        return null;
    }
    
    String toString() {
        return this.fieldMap
    }

    // Dummied out calls
    void readExternal(java.io.ObjectInput obj) { return }
    boolean mapMatchesAny(java.util.Map obj) { return null }
    void writeExternal(java.io.ObjectOutput obj) { return }

}
