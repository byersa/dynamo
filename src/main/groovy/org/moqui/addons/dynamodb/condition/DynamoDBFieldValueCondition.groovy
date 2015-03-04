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
        if (this.field.fieldName in priKeyNames) {
            retVal = this.value
        }
        if (!retVal) {
            List<Node> fieldNodes = ed.getFieldNodes(false, true, false)
            String indexName, fieldName
        
            for (Node nd in fieldNodes) {
                indexName = nd."@index"
                if (indexName) {
                    fieldName = nd."@name"
                    logger.info("DynamoDBFieldValueCondition(66), indexName: ${indexName},fieldName: ${fieldName}, value: ${value}")
                    //TODO: check that compare op is "EQUAL"
                    if (fieldName == this.field.fieldName) {
                        retVal =  this.value
                        logger.info("DynamoDBFieldValueCondition(70), retVal: ${retVal}")
                        break;
                    }
                }
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
        
            for (Node nd in fieldNodes) {
                indexName = nd."@index"
                if (indexName) {
                    fieldName = nd."@name"
        logger.info("DynamoDBFieldValueCondition(66), indexName: ${indexName},fieldName: ${fieldName}, value: ${value}")
                    //TODO: check that compare op is "EQUAL"
                    if (fieldName == this.field.fieldName) {
                        retVal =  this.value
        logger.info("DynamoDBFieldValueCondition(70), retVal: ${retVal}")
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
        AttributeValue attrVal = null
            for (Node nd in fieldNodes) {
                if (nd."@is-range") {
                    fieldName = nd."@name"
                    logger.info("DynamoDBFieldValueCondition(64), indexName: ${indexName},fieldName: ${fieldName}, value: ${value}")
                    //TODO: check that compare op is "EQUAL"
                    if (fieldName == this.field.fieldName) {
                        attrVal =  DynamoDBUtils.getAttributeValue(fieldName, [(fieldName):this.value], ed)
                        logger.info("DynamoDBFieldValueCondition(66), attrVal: ${attrVal}")
                        com.amazonaws.services.dynamodbv2.model.ComparisonOperator compOp = DynamoDBUtils.getComparisonOperator(operator)
                        switch(compOp) {
                            case com.amazonaws.services.dynamodbv2.model.ComparisonOperator.EQ:
                                rangeCond = new RangeKeyCondition().eq(attrVal.getS())
                                break
                            case com.amazonaws.services.dynamodbv2.model.ComparisonOperator.LT:
                                rangeCond = new RangeKeyCondition().lt(attrVal.getS())
                                break
                            case com.amazonaws.services.dynamodbv2.model.ComparisonOperator.GT:
                                rangeCond = new RangeKeyCondition().gt(attrVal.getS())
                                break
                            case com.amazonaws.services.dynamodbv2.model.ComparisonOperator.LE:
                                rangeCond = new RangeKeyCondition().le(attrVal.getS())
                                break
                            case com.amazonaws.services.dynamodbv2.model.ComparisonOperator.GE:
                                rangeCond = new RangeKeyCondition().ge(attrVal.getS())
                                break
                            case com.amazonaws.services.dynamodbv2.model.ComparisonOperator.BEGINS_WITH:
                                rangeCond = new RangeKeyCondition().beginsWith(attrVal.getS())
                                break
                            case com.amazonaws.services.dynamodbv2.model.ComparisonOperator.BETWEEN:
                                rangeCond = new RangeKeyCondition().between(attrVal.getS())
                                break
                            default:
                                rangeCond = new RangeKeyCondition().eq(attrVal.getS())
                                break
                        }
                    }
                }
            }
        return rangeCond
    }

Map <String, Condition> getDynamoDBScanConditionMap() {
        return null;
    }
}
