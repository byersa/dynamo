package org.moqui.impl.entity.dynamodb

import java.sql.Timestamp
import org.moqui.impl.entity.dynamodb.DynamoDBEntityConditionFactoryImpl
import org.moqui.impl.entity.EntityDefinition
import org.moqui.entity.EntityCondition
import org.moqui.entity.EntityCondition.ComparisonOperator.*
import org.moqui.impl.entity.dynamodb.condition.DynamoDBEntityConditionImplBase

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate
import com.amazonaws.services.dynamodbv2.model.AttributeAction
import com.amazonaws.services.dynamodbv2.model.Condition
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.moqui.util.MNode 

class DynamoDBUtils {

    protected final static Logger logger = LoggerFactory.getLogger(DynamoDBUtils.class)
    
    static AttributeValue getAttributeValue(String fieldName, Map<String,?>valueMap, EntityDefinition ed) {
    
        AttributeValue attrVal = new AttributeValue()
        MNode fieldNode = ed.getFieldNode(fieldName)
        String fieldNodeName = fieldNode."@name"
                 logger.info("DynamoDBUtils.getAttributeValue(291) fieldNodeName: ${fieldNodeName}")
        String fieldNodeType = fieldNode."@type"
                 logger.info("DynamoDBUtils.getAttributeValue(293) fieldNodeType: ${fieldNodeType}")
        switch(fieldNodeType) {
            case "id":
            case "id-long":
            case "text-short":
            case "text-medium":
            case "text-long":
            case "text-very-long":
            case "text-indicator":
                 String val = valueMap.get(fieldName)?: ""
                 logger.info("DynamoDBUtils.getAttributeValue(32) val: ${val}")
                 if (val) {
                    attrVal.setS(val)
                 } else {
                     return null
                 }
                 break
            case "number-integer":
                 String val = valueMap.get(fieldName)?: ""
                 logger.info("DynamoDBUtils.getAttributeValue(41) val: ${val}")
                 if (val) {
                    attrVal.setN(val)
                 } else {
                     return null
                 }
                 break
            case "number-decimal":
            case "number-float":
            case "currency-amount":
            case "currency-precise":
                 return attrVal.setN(valueMap.get(fieldName).toString())
                 break
            case "date":
            case "time":
            case "date-time":
                 String dateTimeStr = valueMap.get(fieldName)
                 logger.info("DynamoDBUtils(310) dateTimeStr: ${dateTimeStr}")
                 if (dateTimeStr != null) {
                     Timestamp ts = Timestamp.valueOf(dateTimeStr)
                     logger.info("DynamoDBUtils(312) ts: ${ts.toString()}")
                     attrVal.setS(ts.toString())
                 } else {
                     return null
                 }
                 break
            default:
                 String val = valueMap.get(fieldName)?: ""
                 if (val) {
                     attrVal.setS(val)
                 } else {
                     return null
                 }
        }
        
        return attrVal
    }
    
    static AttributeValueUpdate getAttributeValueUpdate(String fieldName, Map<String,?>valueMap, EntityDefinition ed) {
    
        AttributeValueUpdate attrVal = new AttributeValueUpdate()
        MNode fieldNode = ed.getFieldNode(fieldName)
        String fieldNodeName = fieldNode."@name"
                 logger.info("DynamoDBUtils.getAttributeValue(291) fieldNodeName: ${fieldNodeName}")
        String fieldNodeType = fieldNode."@type"
                 logger.info("DynamoDBUtils.getAttributeValue(293) fieldNodeType: ${fieldNodeType}")
                 
        // do not include hash key field
        if (fieldNodeName in ed.getPkFieldNames()) {
             return null
        }
        // do not include range key field
        String indexName = fieldNode."@index"
        if (indexName) {
                return null
        }
        switch(fieldNodeType) {
            case "id":
            case "id-long":
            case "text-short":
            case "text-medium":
            case "text-long":
            case "text-very-long":
            case "text-indicator":
                 String val = valueMap.get(fieldName)?: ""
                 logger.info("DynamoDBUtils.getAttributeValue(32) val: ${val}")
                 if (val) {
                    attrVal.setValue(new AttributeValue().withS(val))
                 } else {
                     return null
                 }
                 break
            case "number-integer":
                 String val = valueMap.get(fieldName)?: ""
                 logger.info("DynamoDBUtils.getAttributeValue(41) val: ${val}")
                 if (val) {
                    attrVal.setValue(new AttributeValue().withN(val))
                 } else {
                     return null
                 }
                 break
            case "number-decimal":
            case "number-float":
            case "currency-amount":
            case "currency-precise":
                 return attrVal.setN(valueMap.get(fieldName).toString())
                 break
            case "date":
            case "time":
            case "date-time":
                 String dateTimeStr = valueMap.get(fieldName)
                 logger.info("DynamoDBUtils(310) dateTimeStr: ${dateTimeStr}")
                 Timestamp ts = Timestamp.valueOf(dateTimeStr)
                 logger.info("DynamoDBUtils(312) ts: ${ts.toString()}")
                 attrVal.setValue(new AttributeValue().withS(ts.toString()))
                 break
            default:
                 String val = valueMap.get(fieldName)?: ""
                 if (val) {
                    attrVal.setValue(new AttributeValue().withS(val))
                 } else {
                     return null
                 }
        }
        
        return attrVal
    }
    
    static com.amazonaws.services.dynamodbv2.model.ComparisonOperator  getComparisonOperator(EntityCondition.ComparisonOperator op) {
        com.amazonaws.services.dynamodbv2.model.ComparisonOperator retOp = null
        switch(op) {
            case EntityCondition.EQUALS:
                retOp = com.amazonaws.services.dynamodbv2.model.ComparisonOperator.EQ
                break
            case EntityCondition.LESS_THAN:
                retOp = com.amazonaws.services.dynamodbv2.model.ComparisonOperator.LT
                break
            case EntityCondition.GREATER_THAN:
                retOp = com.amazonaws.services.dynamodbv2.model.ComparisonOperator.GT
                break
            case EntityCondition.LESS_THAN_EQUAL_TO:
                retOp = com.amazonaws.services.dynamodbv2.model.ComparisonOperator.LE
                break
            case EntityCondition.GREATER_THAN_EQUAL_TO:
                retOp = com.amazonaws.services.dynamodbv2.model.ComparisonOperator.GE
                break
            case EntityCondition.IN:
                retOp = com.amazonaws.services.dynamodbv2.model.ComparisonOperator.CONTAINS
                break
            case EntityCondition.NOT_IN:
                retOp = com.amazonaws.services.dynamodbv2.model.ComparisonOperator.NOT_CONTAINS
                break
            case EntityCondition.LIKE:
                retOp = com.amazonaws.services.dynamodbv2.model.ComparisonOperator.BEGINS_WITH
                break
            case EntityCondition.BETWEEN:
                retOp = com.amazonaws.services.dynamodbv2.model.ComparisonOperator.BETWEEN
                break
            default:
                retOp = com.amazonaws.services.dynamodbv2.model.ComparisonOperator.EQ
                break
        }

    }

    static String getRangeFieldName(EntityDefinition ed) {
        
        String rangeFieldName
        ArrayList <MNode> fieldNodes = ed.entityNode.getChildren()
        for (MNode nd in fieldNodes) {
                if (nd.attribute("is-range") == "true") {
                    rangeFieldName = nd.attribute("name")
                    break
                }
        }
        return rangeFieldName
    }
}
