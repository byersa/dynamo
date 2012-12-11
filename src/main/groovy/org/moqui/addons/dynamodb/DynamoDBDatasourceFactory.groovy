/*
 * This Work is in the public domain and is provided on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied,
 * including, without limitation, any warranties or conditions of TITLE,
 * NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A PARTICULAR PURPOSE.
 * You are solely responsible for determining the appropriateness of using
 * this Work and assume any risks associated with your use of this Work.
 *
 * This Work includes contributions authored by Al Byers, not as a
 * "work for hire", who hereby disclaims any copyright to the same.
 */
package org.moqui.impl.entity.dynamodb

import org.moqui.impl.entity.EntityDefinition
import org.moqui.impl.entity.EntityFacadeImpl
import org.moqui.entity.EntityFind
import org.moqui.impl.entity.EntityFindImpl
import org.moqui.entity.EntityValue
import org.moqui.impl.entity.EntityValueImpl

import javax.sql.DataSource

import org.moqui.entity.*
import java.sql.Types

import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodb.AmazonDynamoDBClient

/**
 * To use this:
 * 1. add a datasource under the entity-facade element in the Moqui Conf file; for example:
 *      <datasource group-name="transactional_nosql" object-factory="org.moqui.impl.entity.dynamodb.DynamoDBDatasourceFactory">
 *          <inline-other uri="local:runtime/db/orient/transactional" username="moqui" password="moqui"/>
 *      </datasource>
 *
 * 2. to get dynamodb to automatically create the database, add a corresponding "storage" element to the
 *      dynamodb-server-config.xml file
 *
 * 3. add the group-name attribute to entity elements as needed to point them to the new datasource; for example:
 *      group-name="transactional_nosql"
 */
class DynamoDBDatasourceFactory implements EntityDatasourceFactory {
    protected final static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DynamoDBDatasourceFactory.class)

    protected EntityFacadeImpl efi
    protected Node datasourceNode
    protected String tenantId

    protected String uri
    protected String accessKey
    protected String secretAccessKey
    protected AmazonDynamoDBClient dynamoDBClient

    DynamoDBDatasourceFactory() { }

    @Override
    EntityDatasourceFactory init(EntityFacade ef, Node datasourceNode, String tenantId) {
        // local fields
        this.efi = (EntityFacadeImpl) ef
        this.datasourceNode = datasourceNode
        this.tenantId = tenantId

        // init the DataSource
        EntityValue tenant = null
        EntityFacadeImpl defaultEfi = null
        if (this.tenantId != "DEFAULT") {
            defaultEfi = efi.ecfi.getEntityFacade("DEFAULT")
            tenant = defaultEfi.makeFind("moqui.tenant.Tenant").condition("tenantId", this.tenantId).one()
        }

        EntityValue tenantDataSource = null
        EntityList tenantDataSourceXaPropList = null
        if (tenant != null) {
            tenantDataSource = defaultEfi.makeFind("moqui.tenant.TenantDataSource").condition("tenantId", this.tenantId)
                    .condition("entityGroupName", datasourceNode."@group-name").one()
            tenantDataSourceXaPropList = defaultEfi.makeFind("moqui.tenant.TenantDataSourceXaProp")
                    .condition("tenantId", this.tenantId).condition("entityGroupName", datasourceNode."@group-name")
                    .list()
        }

        Node inlineOtherNode = datasourceNode."inline-other"[0]

        Properties moquiInitProperties = new Properties()
        URL initProps = this.class.getClassLoader().getResource("MoquiInit.properties")
        if (initProps != null) { InputStream is = initProps.openStream(); moquiInitProperties.load(is); is.close(); }

        // if there is a system property use that, otherwise from the properties file
        
        accessKey = System.getProperty("moqui.accessKey")
        if(!accessKey) {
            accessKey = moquiInitProperties.getProperty("moqui.accessKey")
        }

        secretAccessKey = System.getProperty("moqui.secretAccessKey")
        if(!secretAccessKey) {
            secretAccessKey = moquiInitProperties.getProperty("moqui.secretAccessKey")
        }

        BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretAccessKey);
        dynamoDBClient = new AmazonDynamoDBClient(credentials)
        
        return this
    }

    AmazonDynamoDBClient getDatabaseDocumentPool() { return dynamoDBClient }

    /** Returns the main database access object for OrientDB.
     * Remember to call close() on it when you're done with it (preferably in a try/finally block)!
     */
    AmazonDynamoDBClient getDatabase() { return dynamoDBClient }

    @Override
    void destroy() {
        return
    }
    @Override
    EntityValue makeEntityValue(String entityName) {
        EntityDefinition entityDefinition = efi.getEntityDefinition(entityName)
        if (!entityDefinition) {
            throw new EntityException("Entity not found for name [${entityName}]")
        }
        return new DynamoDBEntityValue(entityDefinition, efi, this)
    }

    @Override
    EntityFind makeEntityFind(String entityName) {
        return new EntityFindImpl(efi, entityName, this)
    }
    @Override
    DataSource getDataSource() { return null }

}
