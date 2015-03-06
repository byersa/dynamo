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

import org.moqui.impl.entity.dynamodb.DynamoDBEntityConditionFactoryImpl
import org.moqui.entity.EntityCondition
import org.moqui.impl.entity.EntityDefinition
import org.moqui.impl.entity.EntityQueryBuilder
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.Condition
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition

abstract class DynamoDBEntityConditionImplBase implements EntityCondition {
    DynamoDBEntityConditionFactoryImpl ecFactoryImpl

    DynamoDBEntityConditionImplBase(DynamoDBEntityConditionFactoryImpl ecFactoryImpl) {
        this.ecFactoryImpl = ecFactoryImpl
    }


    AttributeValue getDynamoDBHashValue() {
        return null;
    }

    AttributeValue getDynamoDBRangeValue() {
        return null;
    }

    Map <String, Condition> getDynamoDBScanConditionMap() {
        return null;
    }

    Map getDynamoDBFilterExpressionMap(EntityDefinition ed, List skipFieldNames) {
        return null;
    }

    Map <String, String> getDynamoDBIndexCondition(EntityDefinition ed) {
        return null;
    }

    RangeKeyCondition getRangeCondition(EntityDefinition ed) {
        RangeKeyCondition rangeCond = null
        return rangeCond
    }

   @Override
    boolean populateMap(Map<String, ?> map) { return false }
 
}
