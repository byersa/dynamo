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


    AttributeValue getDynamoDBHashValue(EntityDefinition ed) {
        List<String> priKeyNames = ed.getPkFieldNames()
        AttributeValue retVal = null
    logger.info("DynamoDBFieldValueCondition, priKeyNames: ${priKeyNames}, this.field.fieldName:  '${this.field.fieldName}', in priKeyNames: ${this.field.fieldName in priKeyNames}")
        if (this.field.fieldName in priKeyNames) {
            retVal = DynamoDBUtils.getAttributeValue(this.field.fieldName, [(this.field.fieldName):this.value], ed)
        }
        return retVal;
    }

    AttributeValue getDynamoDBRangeValue(EntityDefinition ed) {
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

    Condition getDynamoDBRangeCondition(EntityDefinition ed) {
        List<Node> fieldNodes = ed.getFieldNodes(false, true, false)
        String indexName, fieldName
        Condition retVal = null
        com.amazonaws.services.dynamodbv2.model.ComparisonOperator compOp = null
        AttributeValue attrVal = null
            for (Node nd in fieldNodes) {
                indexName = nd."@index"
                if (indexName) {
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

Map <String, Condition> getDynamoDBScanConditionMap() {
        return null;
    }
}
