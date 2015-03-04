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
import org.moqui.impl.entity.dynamodb.DynamoDBEntityValue
import org.moqui.impl.entity.dynamodb.DynamoDBEntityFind

import javax.sql.DataSource

import org.moqui.entity.*
import java.sql.Types

import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions


import java.util.HashMap
import java.util.Map

import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.regions.Region
import com.amazonaws.regions.Regions
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator
import com.amazonaws.services.dynamodbv2.model.Condition
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest
import com.amazonaws.services.dynamodbv2.model.CreateTableResult
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement
import com.amazonaws.services.dynamodbv2.model.KeyType
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput
import com.amazonaws.services.dynamodbv2.model.PutItemRequest
import com.amazonaws.services.dynamodbv2.model.PutItemResult
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType
import com.amazonaws.services.dynamodbv2.model.ScanRequest
import com.amazonaws.services.dynamodbv2.model.ScanResult
import com.amazonaws.services.dynamodbv2.model.TableDescription
import com.amazonaws.services.dynamodbv2.document.Table
import com.amazonaws.services.dynamodbv2.util.Tables
import com.amazonaws.services.dynamodbv2.document.TableCollection
import com.amazonaws.services.dynamodbv2.model.ListTablesResult
import com.amazonaws.auth.internal.AWS4SignerUtils
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex
import com.amazonaws.services.dynamodbv2.model.Projection
import com.amazonaws.services.dynamodbv2.model.ProjectionType

import com.amazonaws.services.dynamodbv2.document.DynamoDB

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
    protected DynamoDB dynamoDB

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

        //Properties moquiInitProperties = new Properties()
        //URL initProps = this.class.getClassLoader().getResource("MoquiInit.properties")
        //if (initProps != null) { InputStream is = initProps.openStream(); moquiInitProperties.load(is); is.close(); }

        // if there is a system property use that, otherwise from the properties file
        
        accessKey = System.getProperty("dynamodb.accessKey")
        if(!accessKey) {
           // accessKey = moquiInitProperties.getProperty("moqui.accessKey")
            accessKey = inlineOtherNode."@dynamodb-accessKey"
        }

        secretAccessKey = System.getProperty("dynamodb.secretAccessKey")
        if(!secretAccessKey) {
            //secretAccessKey = moquiInitProperties.getProperty("moqui.secretAccessKey")
            secretAccessKey = inlineOtherNode."@dynamodb-secretAccessKey"
        }

        logger.info("accessKey: ${accessKey}, secretAccessKey: ${secretAccessKey}")
        BasicAWSCredentials credentials = new BasicAWSCredentials(accessKey, secretAccessKey);
        logger.info("credentials: ${credentials}")
        dynamoDBClient = new AmazonDynamoDBClient(credentials)
        logger.info("dynamoDBClient: ${dynamoDBClient}")
        Region usEast1 = Region.getRegion(Regions.US_EAST_1)
        dynamoDBClient.setRegion(usEast1)
        dynamoDB = new DynamoDB(dynamoDBClient) 
        return this
    }

    AmazonDynamoDBClient getDatabaseDocumentPool() { return dynamoDBClient}
    AmazonDynamoDBClient getDynamoDBClient() { return dynamoDBClient}

    /** Returns the main database access object for OrientDB.
     * Remember to call close() on it when you're done with it (preferably in a try/finally block)!
     */
    DynamoDB getDatabase() { return dynamoDB}

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
        return new DynamoDBEntityFind(efi, entityName, this)
    }
    @Override
    DataSource getDataSource() { return null }

    @Override
    void checkAndAddTable(java.lang.String tableName) {

            logger.info("checking: ${tableName}")
            if( !Tables.doesTableExist(dynamoDBClient, tableName)) {
                this.createTable(tableName)
            }
        return
    }

    void createTable(tableName) {

                logger.info("building: ${tableName}")
                def ed = efi.getEntityDefinition(tableName)
                ArrayList<AttributeDefinition> attributeDefinitions= new ArrayList()
                ArrayList<KeySchemaElement> keySchema = new ArrayList()
                List <Node> fieldNodes = ed.getFieldNodes(true, true, false)
                String hashFieldName = ""
                fieldNodes.each() { nd ->
                    logger.info("building node: ${nd}")
                    String nodeName = nd."@name"
                    if (ed.isPkField(nodeName)) {
                         logger.info("primaryKey: ${nodeName}")
                         hashFieldName = nodeName
                         keySchema.add(new KeySchemaElement().withAttributeName(nodeName).withKeyType(KeyType.HASH))
                         attributeDefinitions.add(new AttributeDefinition().withAttributeName(nodeName).withAttributeType("S"))
                    }
                    if (nd."@is-range" == "true") {
                         logger.info("rangeKey: ${nodeName}")
                         keySchema.add(new KeySchemaElement().withAttributeName(nodeName).withKeyType(KeyType.RANGE))
                         attributeDefinitions.add(new AttributeDefinition().withAttributeName(nodeName).withAttributeType("S"))
                    }
                    String attrType
                   switch(nd."@type") {
                        case "id":
                        case "id-long":
                        case "text-short":
                        case "text-medium":
                        case "text-long":
                        case "text-very-long":
                        case "text-indicator":
                        case "number-integer":
                        case "number-decimal":
                        case "number-float":
                        case "currency-amount":
                        case "currency-precise":
                             attrType = "N"
                             break
                        case "date":
                        case "time":
                        case "date-time":
                             attrType = "S"
                             break
                        default:
                             attrType = "S"
                    }
                }
                String indexFieldName
                GlobalSecondaryIndex secondaryIndex 
                List <GlobalSecondaryIndex> secondaryIndices = new ArrayList()
                for (Node indexNode in ed.entityNode."index") {
                    for (Node indexFieldNode in indexNode."index-field") {
                        indexFieldName = indexFieldNode."@name"
                        secondaryIndex = new GlobalSecondaryIndex()
                            .withIndexName(indexNode."@name")
                            .withProvisionedThroughput(new ProvisionedThroughput()
                                .withReadCapacityUnits((long) 10)
                                .withWriteCapacityUnits((long) 1))
                                .withProjection(new Projection().withProjectionType(ProjectionType.ALL))
                        ArrayList<KeySchemaElement> indexKeySchema = new ArrayList()
                          
//                        indexKeySchema.add(new KeySchemaElement()
//                              .withAttributeName(hashFieldName)
//                              .withKeyType(KeyType.HASH))
                        indexKeySchema.add(new KeySchemaElement()
                              .withAttributeName(indexFieldName)
                              .withKeyType(KeyType.HASH))
                          
                        secondaryIndex.setKeySchema(indexKeySchema)
                        secondaryIndices.add(secondaryIndex)

                        attributeDefinitions.add(new AttributeDefinition().withAttributeName(indexFieldName).withAttributeType("S"))
                    }   
                }
 
                logger.info("creating: ${tableName}")
                CreateTableRequest request = new CreateTableRequest()
		     .withTableName(tableName)
		     .withKeySchema(keySchema)
		     .withAttributeDefinitions(attributeDefinitions)
		     .withProvisionedThroughput(new ProvisionedThroughput()
		         .withReadCapacityUnits(1L)
			     .withWriteCapacityUnits(1L))

                if (secondaryIndices) {
                    logger.info("hasSecondaryIndices: ${secondaryIndices}")
                    request.setGlobalSecondaryIndexes(secondaryIndices)
                }
                CreateTableResult createTableResult = dynamoDBClient.createTable(request)
                    logger.info("isActive: ${createTableResult.getTableDescription()}")
        return
    }
}
