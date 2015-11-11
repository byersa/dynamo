/**
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

import org.moqui.entity.EntityCondition
import org.moqui.impl.entity.dynamodb.DynamoDBEntityConditionFactoryImpl

import static org.moqui.entity.EntityCondition.ComparisonOperator.*
import org.moqui.impl.entity.EntityDefinition
import org.moqui.impl.entity.dynamodb.DynamoDBUtils
import org.moqui.impl.entity.dynamodb.condition.DynamoDBEntityConditionImplBase
import org.moqui.impl.entity.condition.ConditionField
import org.moqui.impl.entity.condition.FieldValueCondition

import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.Condition
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator

import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition

class DynamoDBFieldValueCondition extends DynamoDBEntityConditionImplBase {

    protected final static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DynamoDBFieldValueCondition.class)
    protected Class localClass = null
    protected ConditionField field
    protected EntityCondition.ComparisonOperator operator
    protected Object value
    protected boolean ignoreCase = false


    DynamoDBFieldValueCondition(DynamoDBEntityConditionFactoryImpl ecFactoryImpl,
            ConditionField field, EntityCondition.ComparisonOperator operator, Object value) {
        super(ecFactoryImpl)
        this.field = field
        this.operator = operator ?: EQUALS
        this.value = value
    }

    boolean mapMatches(Map<String, ?> map) { return null }
    EntityCondition ignoreCase() { return null }


    String getDynamoDBHashValue(EntityDefinition ed) {
        // This method first looks to see if the field name in the condition (this.field.fieldName) is on the primary key def
        // if not, then it could be a secondary key, so look for all indices defined by a "<index..." and see if their field names
        // match the condition field name
        List<String> priKeyNames = ed.getPkFieldNames()
        String retVal = null
        logger.info("DynamoDBFieldValueCondition, priKeyNames: ${priKeyNames}, this.field.fieldName:  '${this.field.fieldName}', in priKeyNames: ${this.field.fieldName in priKeyNames}")
        for(String fieldName in priKeyNames) {
            logger.info("DynamoDBFieldValueCondition(getDynamoDBHashValue), fieldName: ${fieldName}, this.field.fieldName: ${this.field.fieldName}")
            if( fieldName == this.field.fieldName) {
                if(operator == org.moqui.entity.EntityCondition.ComparisonOperator.EQUALS) {
                     retVal =  this.value
                }
                break
            }
        }
        return retVal;
    }

    AttributeValue getDynamoDBHashAttributeValue(EntityDefinition ed) {
        List<String> priKeyNames = ed.getPkFieldNames()
        AttributeValue retVal = null
    logger.info("DynamoDBFieldValueCondition, priKeyNames: ${priKeyNames}, this.field.fieldName:  '${this.field.fieldName}', in priKeyNames: ${this.field.fieldName in priKeyNames}")
        if (this.field.fieldName in priKeyNames) {
            retVal = DynamoDBUtils.getAttributeValue(this.field.fieldName, [(this.field.fieldName):this.value], ed)
        }
        return retVal;
    }

    String getDynamoDBRangeValue(EntityDefinition ed) {
        List<Node> fieldNodes = ed.getFieldNodes(false, true, false)
        String indexName, fieldName
        String retVal = null
        def isRange
        
            for (Node nd in fieldNodes) {
                fieldName = nd."@name"
                isRange = nd."@is-range"
                logger.info("DynamoDBFieldValueCondition(100), this.field.fieldName: ${this.field.fieldName},fieldName: ${fieldName}, value: ${value}")
                logger.info("DynamoDBFieldValueCondition(101), isRange: ${isRange}")
                if (nd."@is-range" == "true") {
                //indexName = nd."@index"
                //if (indexName) {
                    //TODO: check that compare op is "EQUAL"
                    if (fieldName == this.field.fieldName) {
                        retVal =  this.value
        logger.info("DynamoDBFieldValueCondition(110), retVal: ${retVal}")
                        break;
                    }
                }
            }
        return retVal
    }

    AttributeValue getDynamoDBRangeAttributeValue(EntityDefinition ed) {
        List<Node> fieldNodes = ed.getFieldNodes(false, true, false)
        String indexName, fieldName
        AttributeValue retVal = null
        
            for (Node nd in fieldNodes) {
                indexName = nd."@index"
                if (indexName) {
                    fieldName = nd."@name"
        logger.info("DynamoDBFieldValueCondition(66), indexName: ${indexName},fieldName: ${fieldName}, value: ${value}")
                    //TODO: check that compare op is "EQUAL"
                    if (fieldName == this.field.fieldName) {
                        retVal =  DynamoDBUtils.getAttributeValue(fieldName, [(fieldName):this.value], ed)
        logger.info("DynamoDBFieldValueCondition(70), retVal: ${retVal}")
                        break;
                    }
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
                if (nd."@is-range") {
                    fieldName = nd."@name"
        logger.info("DynamoDBFieldValueCondition(64), indexName: ${indexName},fieldName: ${fieldName}, value: ${value}")
                    //TODO: check that compare op is "EQUAL"
                    if (fieldName == this.field.fieldName) {
                        attrVal =  DynamoDBUtils.getAttributeValue(fieldName, [(fieldName):this.value], ed)
        logger.info("DynamoDBFieldValueCondition(66), attrVal: ${attrVal}")
                        compOp = DynamoDBUtils.getComparisonOperator(operator)
                        retVal = new Condition()
                        retVal.setAttributeValueList([attrVal])
                        retVal.setComparisonOperator(compOp)
                        break;
                    }
                }
            }
        return retVal
    }

    RangeKeyCondition getRangeCondition(EntityDefinition ed) {
        List<Node> fieldNodes = ed.getFieldNodes(false, true, false)
        String indexName, fieldName
        RangeKeyCondition rangeCond = null
        String attrVal = null
        logger.info("DynamoDBFieldValueCondition , operator: ${operator}, this.field.fieldName: ${this.field.fieldName}, this.value: ${value}")
            for (Node nd in fieldNodes) {
                if (nd."@is-range") {
                    fieldName = nd."@name"
                    //TODO: check that compare op is "EQUAL"
                    if (fieldName == this.field.fieldName) {
                        logger.info("DynamoDBFieldValueCondition , fieldName: ${fieldName}")
                        com.amazonaws.services.dynamodbv2.model.ComparisonOperator compOp = DynamoDBUtils.getComparisonOperator(operator)
                        logger.info("DynamoDBFieldValueCondition , compOp: ${compOp}")
                        switch(compOp) {
                            case com.amazonaws.services.dynamodbv2.model.ComparisonOperator.EQ:
                                rangeCond = new RangeKeyCondition(fieldName).eq(value)
                                break
                            case com.amazonaws.services.dynamodbv2.model.ComparisonOperator.LT:
                                rangeCond = new RangeKeyCondition(fieldName).lt(value)
                                break
                            case com.amazonaws.services.dynamodbv2.model.ComparisonOperator.GT:
                                rangeCond = new RangeKeyCondition(fieldName).gt(value)
                                break
                            case com.amazonaws.services.dynamodbv2.model.ComparisonOperator.LE:
                                rangeCond = new RangeKeyCondition(fieldName).le(value)
                                break
                            case com.amazonaws.services.dynamodbv2.model.ComparisonOperator.GE:
                                rangeCond = new RangeKeyCondition(fieldName).ge(value)
                                break
                            case com.amazonaws.services.dynamodbv2.model.ComparisonOperator.BEGINS_WITH:
                                rangeCond = new RangeKeyCondition(fieldName).beginsWith(value)
                                break
                            case com.amazonaws.services.dynamodbv2.model.ComparisonOperator.BETWEEN:
                                rangeCond = new RangeKeyCondition(fieldName).between(value)
                                break
                            default:
                                rangeCond = new RangeKeyCondition(fieldName).eq(value)
                                break
                        }
                    }
                }
            }
        logger.info("DynamoDBFieldValueCondition , rangeCond: ${rangeCond}")
        if (rangeCond) {
        logger.info("DynamoDBFieldValueCondition , rangeCond: ${rangeCond.getKeyCondition()}")
        }
        return rangeCond
    }

    Map getDynamoDBFilterExpressionMap(EntityDefinition ed, List skipFieldNames) {
        Map retMap
        logger.info("in getDynamoDBFilterExpressionMap, skipFieldNames: ${skipFieldNames}")
            logger.info("in getDynamoDBFilterExpressionMap, value: ${value}, this.field.fieldName: ${this.field.fieldName}")
        List<Node> fieldNodes = ed.getFieldNodes(true, true, false)
        String indexName, fieldName, fieldValue
        String filterExpression = ""
        String customExpression = ""
        Map attrNameMap = new HashMap()
        Map attrValueMap = new HashMap()
        for (Node nd in fieldNodes) {
            fieldName = nd."@name"
            customExpression = ""
            logger.info("in getDynamoDBFilterExpressionMap, fieldName: ${fieldName}, this.field.fieldName: ${this.field.fieldName}")
            if (fieldName == this.field.fieldName && skipFieldNames.indexOf(fieldName) < 0) {
                    String op 
                    switch(this.operator) {
                        case org.moqui.entity.EntityCondition.ComparisonOperator.GREATER_THAN:
                            op = " > "
                            break
                        case org.moqui.entity.EntityCondition.ComparisonOperator.GREATER_THAN_EQUAL_TO:
                            op = " >= "
                            break
                        case org.moqui.entity.EntityCondition.ComparisonOperator.LESS_THAN:
                            op = com.amazonaws.services.dynamodbv2.model.ComparisonOperator.LT
                            op = " < "
                            break
                        case org.moqui.entity.EntityCondition.ComparisonOperator.LESS_THAN_EQUAL_TO:
                            op = com.amazonaws.services.dynamodbv2.model.ComparisonOperator.LE
                            op = " <= "
                            break
                        case org.moqui.entity.EntityCondition.ComparisonOperator.LIKE:
                            // FIXME: this needs to use conditional expression function, see http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference
                            op = com.amazonaws.services.dynamodbv2.model.ComparisonOperator.BEGINS_WITH
                            op = " >= "
                            customExpression = " contains(#${field}, :${field}) "
                            //customExpression = " begins_with(#{field}, :{field}) "
                            break
                        case org.moqui.entity.EntityCondition.ComparisonOperator.NOT_EQUAL:
                            op = " <> "
                            break
                        default:
                            op = " = "
                    }
                    if (filterExpression) { filterExpression += " AND " }
                    if (customExpression) {
                        filterExpression += customExpression
                    } else {
                        filterExpression += "#${field} ${op} :${field} "
                    }
                    logger.info("in getDynamoDBFilterExpressionMap, filterExpression: ${filterExpression}")
                    attrNameMap.put("#" + this.field.fieldName, this.field.fieldName)
                    attrValueMap.put(":" + this.field.fieldName, this.value)
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
                for (Node indexNode in ed.entityNode."index") {
                        logger.info("DynamoDBMapCondition, getDynamoDBIndexValue, indexNode: ${indexNode}")
                    String indexFieldName
                    for (Node indexFieldNode in indexNode."index-field") {
                        indexFieldName = indexFieldNode."@name"
                        logger.info("DynamoDBMapCondition, getDynamoDBIndexValue, indexFieldName: ${indexFieldName}")
                        if( indexFieldName == this.field.fieldName) {
                            retVal = new HashMap()
                            // indexNode."@name" has the secondary index name that DynamoDB knows
                            retVal.put("indexName", indexNode."@name")
                            retVal.put("indexFieldName", indexFieldName)
                            retVal.put("indexFieldValue", value)
                            break
                        }
                    }   
                }
        return retVal;
    }

Map <String, Condition> getDynamoDBScanConditionMap() {
        return null;
    }
}
