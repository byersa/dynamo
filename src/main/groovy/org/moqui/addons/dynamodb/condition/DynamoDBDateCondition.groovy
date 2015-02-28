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

import org.moqui.entity.EntityCondition
import java.sql.Timestamp
import org.moqui.impl.entity.dynamodb.DynamoDBEntityConditionFactoryImpl
import org.moqui.impl.entity.EntityQueryBuilder
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.Condition

class DynamoDBDateCondition extends DynamoDBEntityConditionImplBase {
    protected String fromFieldName
    protected String thruFieldName
    protected Timestamp compareStamp

    DynamoDBDateCondition(DynamoDBEntityConditionFactoryImpl ecFactoryImpl,
            String fromFieldName, String thruFieldName, Timestamp compareStamp) {
        super(ecFactoryImpl)
        this.fromFieldName = fromFieldName ?: "fromDate"
        this.thruFieldName = thruFieldName ?: "thruDate"
        this.compareStamp = compareStamp
    }


    protected DynamoDBEntityConditionImplBase makeCondition() {
        return (DynamoDBEntityConditionImplBase) ecFactoryImpl.makeCondition(
            ecFactoryImpl.makeCondition(
                ecFactoryImpl.makeCondition(thruFieldName, EntityCondition.EQUALS, null),
                EntityCondition.JoinOperator.OR,
                ecFactoryImpl.makeCondition(thruFieldName, EntityCondition.GREATER_THAN, compareStamp)
            ),
            EntityCondition.JoinOperator.AND,
            ecFactoryImpl.makeCondition(
                ecFactoryImpl.makeCondition(fromFieldName, EntityCondition.EQUALS, null),
                EntityCondition.JoinOperator.OR,
                ecFactoryImpl.makeCondition(fromFieldName, EntityCondition.LESS_THAN_EQUAL_TO, compareStamp)
            )
        )
    }

    boolean mapMatches(Map<String, ?> map) { return null }
    EntityCondition ignoreCase() { return null }


    AttributeValue getDynamoDBHashValue() {
        return null;
    }

    AttributeValue getDynamoDBRangeValue() {
        return null;
    }

    Map <String, Condition> getDynamoDBScanConditionMap() {
        return null;
    }
}
