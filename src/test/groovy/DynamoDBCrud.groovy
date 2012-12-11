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
import org.moqui.Moqui
import org.moqui.impl.entity.dynamodb.DynamoDBDatasourceFactory
import org.moqui.impl.entity.dynamodb.DynamoDBEntityValue

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
    }

    def cleanupSpec() {
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

    def "create and find Example TEST1"() {
        when:
        DynamoDBEntityValue ev = ddf.makeEntityValue("Contact")
        logger.info("TEST1 ev: ${ev}")
        ev.setAll([partyId:"TEST1", lastName:"Name", firstName:"Test"])
        logger.info("TEST1 ev(2): ${ev}")
        logger.info("TEST1 evgetDbValueMap: ${ev.getDbValueMap()}")
        logger.info("TEST1 ev(2)getValueMap: ${ev.getValueMap()}")
        ev.create()

        then:
        EntityValue example = ddf.makeEntityFind("Contact").condition("partyId", "TEST1").one()
        example.exampleName == "Name"
    }

/*
    def "update Example TEST1"() {
        when:
        EntityValue example = ec.entity.makeFind("Example").condition("exampleId", "TEST1").one()
        example.exampleName = "Test Name 2"
        example.update()

        then:
        EntityValue exampleCheck = ec.entity.makeFind("Example").condition([exampleId:"TEST1"]).one()
        exampleCheck.exampleName == "Test Name 2"
    }

    def "delete Example TEST1"() {
        when:
        ec.entity.makeFind("Example").condition([exampleId:"TEST1"]).one().delete()

        then:
        EntityValue exampleCheck = ec.entity.makeFind("Example").condition([exampleId:"TEST1"]).one()
        exampleCheck == null
    }
    */
}
