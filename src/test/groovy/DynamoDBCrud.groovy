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

import spock.lang.*

import org.moqui.context.ExecutionContext
import org.moqui.entity.EntityValue
import org.moqui.entity.EntityCondition
import org.moqui.entity.EntityFind
import org.moqui.entity.EntityList
import org.moqui.Moqui
import org.moqui.impl.entity.dynamodb.DynamoDBDatasourceFactory
import org.moqui.impl.entity.dynamodb.DynamoDBEntityValue
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator

class DynamoDBCrud extends Specification {
    @Shared
    ExecutionContext ec
    
    @Shared
    DynamoDBDatasourceFactory ddf
    
    @Shared
    protected final static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DynamoDBEntityValue.class)
    
    def setupSpec() {
        // init the framework, get the ec
        ec = Moqui.getExecutionContext()
        ddf = ec.entity.datasourceFactoryByGroupMap.get("transactional_nosql")
        logger.info("TEST1 ddf: ${ddf}")
        ec.artifactExecution.disableAuthz()
        DynamoDBEntityValue ev = ddf.makeEntityValue("Property")
        ev.setAll([partyId:"TEST5",  propertyId:"2012-12-12 00:00:00", description:"Overpriced", address1:"111 Regent Court", city:"Orem"]).create()
        ev.setAll([partyId:"TEST5",  propertyId:"2012-12-13 00:00:00", description:"UnderPriced", address1:"222 Regent Court", city:"Provo"]).create()
        ev.setAll([partyId:"TEST5",  propertyId:"2012-12-14 00:00:00", description:"Just Right", address1:"333 Regent Court", city:"Lindon"]).create()
    }

    def cleanupSpec() {
        ec.artifactExecution.enableAuthz()
        ec.entity.makeFind("Property").condition([partyId:"TEST5", propertyId:"2012-12-12 00:00:00"]).one().delete()
        ec.entity.makeFind("Property").condition([partyId:"TEST5", propertyId:"2012-12-13 00:00:00"]).one().delete()
        ec.entity.makeFind("Property").condition([partyId:"TEST5", propertyId:"2012-12-14 00:00:00"]).one().delete()
        ec.destroy()
    }

    def setup() {
        ec.artifactExecution.disableAuthz()
        //ec.transaction.begin(null)
    }

    def cleanup() {
        ec.artifactExecution.enableAuthz()
        //ec.transaction.commit()
    }

/*
    def "create and find Contact TEST1"() {
        when:
        DynamoDBEntityValue ev = ddf.makeEntityValue("Contact")
        ev.setAll([partyId:"TEST1", lastName:"Name", firstName:"Test"]).create()

        then:
        EntityValue example = ddf.makeEntityFind("Contact").condition(["partyId": "TEST1"]).one()
        example?.lastName == "Name"
    }



    def "update Contact TEST1"() {
        when:
        DynamoDBEntityValue ev = ddf.makeEntityValue("Contact")
        logger.info("TEST1 Contact: ${ev}, isDynamo: ${ev instanceof DynamoDBEntityValue}")
        ev.setAll([partyId:"TEST1", lastName:"NameU", firstName:"TestUpd"]).update()

        then:
        EntityValue example = ec.entity.makeFind("Contact").condition([partyId:"TEST2"]).one()
        example.lastName == "NameU"
    }

    def "delete Contact TEST1"() {
        when:
        ec.entity.makeFind("Contact").condition([partyId:"TEST1"]).one().delete()

        then:
        EntityValue exampleCheck = ec.entity.makeFind("Contact").condition([partyId:"TEST1"]).one()
        logger.info("TEST1 Contact: exampleCheck: ${exampleCheck}")
        exampleCheck == null
    }

    def "create and find Property Prop1"() {
        when:
        DynamoDBEntityValue ev = ddf.makeEntityValue("Property")
        ev.setAll([partyId:"TEST1", propertyId:"2012-12-12 00:00:00", description:"Overpriced", address1:"1151 Regent", city:"Orem"]).create()
        logger.info("TEST1 Property: ${ev}")

        then:
        EntityValue example = ddf.makeEntityFind("Property").condition(["partyId": "TEST1", propertyId:"2012-12-12 00:00:00"]).one()
        example?.city == "Orem"
    }
    def "update Property TEST1"() {
        when:
        DynamoDBEntityValue ev = ddf.makeEntityValue("Property")
        logger.info("TEST1 Property: ${ev}, isDynamo: ${ev instanceof DynamoDBEntityValue}")
        ev.setAll([partyId:"TEST1", propertyId:"2012-12-12 00:00:00", description:"Overpriced", address1:"1151 Regent Court", city:"Provo"]).update()

        then:
        EntityValue example = ec.entity.makeFind("Property").condition([partyId:"TEST1", propertyId:"2012-12-12 00:00:00"]).one()
        example.city == "Provo"
    }

    def "delete Property TEST1"() {
        when:
        ec.entity.makeFind("Property").condition([partyId:"TEST1", propertyId:"2012-12-12 00:00:00"]).one().delete()

        then:
        EntityValue exampleCheck = ec.entity.makeFind("Property").condition([partyId:"TEST1", propertyId:"2012-12-12 00:00:00"]).one()
        logger.info("TEST1 Property: exampleCheck: ${exampleCheck}")
        exampleCheck == null
    }

    def "create and find Contact TEST2"() {
        when:
        DynamoDBEntityValue ev = ddf.makeEntityValue("Contact")
        ev.setAll([partyId:"TEST2", lastName:"Name", firstName:"Test"]).create()
        logger.info("TEST2, ev: ${ev}")
        then:
        EntityValue example = ddf.makeEntityFind("Contact").condition("partyId", EntityCondition.ComparisonOperator.EQUALS ,"TEST2").one()
        logger.info("TEST2, example: ${example}")
        example?.lastName == "Name"
    }

    def "update Contact TEST2"() {
        when:
        DynamoDBEntityValue ev = ddf.makeEntityValue("Contact")
        logger.info("TEST2 Contact: ${ev}, isDynamo: ${ev instanceof DynamoDBEntityValue}")
        ev.setAll([partyId:"TEST2", lastName:"NameU", firstName:"TestUpd"]).update()

        then:
        EntityValue example = ec.entity.makeFind("Contact").condition([partyId:"TEST2"]).one()
        example.lastName == "NameU"
    }

    def "delete Contact TEST1"() {
        when:
        ec.entity.makeFind("Contact").condition([partyId:"TEST2"]).one().delete()

        then:
        EntityValue exampleCheck = ec.entity.makeFind("Contact").condition([partyId:"TEST2"]).one()
        logger.info("TEST1 Contact: exampleCheck: ${exampleCheck}")
        exampleCheck == null
    }
    */
    def "test list query on Property TEST5 hash only"() {
        when:
        List <EntityValue> propList = ddf.makeEntityFind("Property").condition(["partyId": "TEST5"]).list()
        logger.info("TEST5 Property list hash only: ${propList}")

        then:
        propList.size() == 3
    }
    def "test list query on Property TEST5 date range 1"() {
        when:
        EntityList propList = ddf.makeEntityFind("Property").condition(["partyId": "TEST5", propertyId:"2012-12-12 00:00:00"]).list()
        logger.info("TEST5 Property list date range 1: ${propList}")

        then:
        propList.size() == 1
    }
    def "test list query on Property TEST5 date range 2b"() {
        when:
        EntityFind entFind = ddf.makeEntityFind("Property").condition(["partyId": "TEST5"])
        entFind.condition("propertyId", EntityCondition.ComparisonOperator.EQUALS, "2012-12-13 00:00:00")
        logger.info("TEST5 whereCondition: ${entFind.getWhereEntityCondition()}")
        //entFind.condition([propertyId:"2012-12-14 00:00:00"])
        EntityList propList = entFind.list()
        logger.info("TEST5 Property list date range 2b: ${propList}")

        then:
        propList.size() == 1
    }
    def "test list query on Property TEST5 date range 2 GE-LE"() {
        when:
        EntityFind entFind = ddf.makeEntityFind("Property").condition(["partyId": "TEST5"])
        entFind.condition("propertyId", EntityCondition.ComparisonOperator.GREATER_THAN_EQUAL_TO, "2012-12-13 00:00:00")
        entFind.condition("propertyId", EntityCondition.ComparisonOperator.LESS_THAN_EQUAL_TO, "2012-12-14 00:00:00")
        logger.info("TEST5 whereCondition: ${entFind.getWhereEntityCondition()}")
        //entFind.condition([propertyId:"2012-12-14 00:00:00"])
        EntityList propList = entFind.list()
        logger.info("TEST5 Property list date range 2c: ${propList}")

        then:
        propList.size() == 2
    }

}
