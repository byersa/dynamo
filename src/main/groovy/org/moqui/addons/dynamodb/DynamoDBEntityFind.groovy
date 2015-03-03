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
package org.moqui.impl.entity.dynamodb

import java.sql.ResultSet
import java.sql.Connection
import java.sql.SQLException

import org.moqui.entity.EntityDynamicView

import org.moqui.entity.EntityCondition.JoinOperator
import org.moqui.entity.EntityList
import org.moqui.entity.EntityValue
import org.moqui.entity.EntityFacade
import org.moqui.entity.EntityListIterator
import org.moqui.entity.EntityException
import org.moqui.entity.EntityCondition
import org.moqui.impl.entity.EntityFacadeImpl
import org.moqui.impl.entity.EntityDefinition
import org.moqui.impl.entity.EntityListImpl
import org.moqui.impl.entity.EntityFindBuilder

import org.moqui.impl.entity.dynamodb.condition.DynamoDBEntityConditionImplBase
import org.moqui.impl.entity.dynamodb.DynamoDBEntityValue
import org.moqui.impl.entity.dynamodb.DynamoDBEntityFindBase
import org.moqui.impl.context.ExecutionContextFactoryImpl

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient
import com.amazonaws.services.dynamodbv2.model.AttributeAction
import com.amazonaws.services.dynamodbv2.model.AttributeValue
import com.amazonaws.services.dynamodbv2.model.AttributeValueUpdate
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult
import com.amazonaws.services.dynamodbv2.model.ExpectedAttributeValue
import com.amazonaws.services.dynamodbv2.model.GetItemRequest
import com.amazonaws.services.dynamodbv2.model.GetItemResult
//import com.amazonaws.services.dynamodbv2.model.Key
import com.amazonaws.services.dynamodbv2.model.PutItemRequest
import com.amazonaws.services.dynamodbv2.model.PutItemResult
import com.amazonaws.services.dynamodbv2.model.ReturnValue
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult
import com.amazonaws.services.dynamodbv2.model.QueryRequest
import com.amazonaws.services.dynamodbv2.model.QueryResult
import com.amazonaws.services.dynamodbv2.model.Condition
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator

import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputExceededException
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException
import com.amazonaws.services.dynamodbv2.model.InternalServerErrorException
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException
import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonServiceException

import com.amazonaws.services.dynamodbv2.document.DynamoDB
import com.amazonaws.services.dynamodbv2.document.Table
import com.amazonaws.services.dynamodbv2.document.PrimaryKey
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec
import com.amazonaws.services.dynamodbv2.document.GetItemOutcome
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.ItemCollection
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec
import com.amazonaws.services.dynamodbv2.document.QueryOutcome
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition

class DynamoDBEntityFind extends DynamoDBEntityFindBase {
    protected final static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DynamoDBEntityFind.class)
    protected AmazonDynamoDBClient client
    protected DynamoDB dynamoDB
    protected DynamoDBDatasourceFactory ddf

    DynamoDBEntityFind(EntityFacadeImpl efi, String entityName, DynamoDBDatasourceFactory ddf) {
        super(efi, entityName)
        this.ddf = ddf
        this.dynamoDB = ddf.getDatabase()
    }

    // ======================== Run Find Methods ==============================

    EntityValue one() throws EntityException {

        EntityDefinition ed = this.getEntityDef()
        //Map<String,Object> valMap = getValueMap()
        // if over-constrained (anything in addition to a full PK), just use the full PK
        logger.info("DynamoDBEntityFind.one (73), simpleAndMap: ${simpleAndMap}")
        EntityCondition whereCondition = this.getWhereEntityCondition()
            logger.info("DynamoDBFindEntity(96), whereCondition: ${whereCondition.toString()}")
        /*
        if (ed.containsPrimaryKey(simpleAndMap)) {
            whereCondition = (DynamoDBEntityConditionImplBase) this.conditionFactory.makeCondition(simpleAndMap)
        } else {
            throw(new EntityException("Primary key not contained in ${simpleAndMap}"))
        }
        */


        DynamoDBEntityValue entValue = null
        try {
            AttributeValue hashAttrVal = whereCondition.getDynamoDBHashValue(ed)
            logger.info("DynamoDBEntityFind.one (107), hashAttrVal: ${hashAttrVal.toString()}")
            String hashFieldName = ed.getFieldNames(true, false, false)[0]
            PrimaryKey primaryKey = new PrimaryKey(hashFieldName, hashAttrVal.getS())
            RangeKeyCondition rangeCondition = whereCondition.getRangeCondition(ed)
            logger.info("DynamoDBFindEntity(111), rangeCondition: ${rangeCondition}")
            if (rangeCondition) {
                AttributeValue rangeAttrValue = rangeCondition.getAttibuteValueList()[0]
                String rangeFieldName = DynamoDBUtils.getRangeFieldName(ed)
                primaryKey.addComponent(rangeFieldName, rangeAttrValue.getS())
            }
            GetItemSpec getItemSpec = new GetItemSpec().withPrimaryKey(primaryKey)

            String entName = ed.getFullEntityName()
            Table table = dynamoDB.getTable(entName)
            logger.info("DynamoDBEntityFind.one table: ${table}")
            GetItemOutcome getItemOutcome = table.getItemOutcome(getItemSpec)
            Item item = getItemOutcome.getItem()
            
            logger.info("DynamoDBEntityFind.one item: ${item}")
            Map<java.lang.String,java.lang.Object> itemAsMap
            if (item) {
                itemAsMap = item.asMap()
                logger.info("DynamoDBEntityFind.list itemAsMap: ${itemAsMap}")
                entValue = ddf.makeEntityValue(entName) 
                //entValue.buildEntityValueMap()
                entValue.setAll(itemAsMap)
            } else {
                entValue = null
            }
            
        } catch(ProvisionedThroughputExceededException e1) {
            throw new EntityException(e1.getMessage())
        } catch(ConditionalCheckFailedException e2) {
            throw new EntityException(e2.getMessage())
        } catch(InternalServerErrorException e3) {
            throw new EntityException(e3.getMessage())
        } catch(ResourceNotFoundException e4) {
            throw new EntityException(e4.getMessage())
        } catch(AmazonClientException e5) {
            throw new EntityException(e5.getMessage())
        } catch(AmazonServiceException e6) {
            throw new EntityException(e6.getMessage())
        }finally {
        }
        return entValue
    }

    EntityListIterator iteratorExtended(DynamoDBEntityConditionImplBase whereCondition, DynamoDBEntityConditionImplBase havingCondition,
                                        List<String> orderByExpanded) throws EntityException {
        EntityDefinition ed = this.getEntityDef()
        // If has primary hash key and range condition, then do query
        // else, do scan
    }


    /** @see org.moqui.entity.EntityFind#list() */
    EntityList list() throws EntityException {
        long startTime = System.currentTimeMillis()
        EntityDefinition ed = this.getEntityDef()
        List retList = null
        DynamoDBEntityValue entValue = null
        EntityList entList = new EntityListImpl(this.efi)
        try {
            DynamoDBEntityConditionImplBase whereCondition = this.getWhereEntityCondition()
            logger.info("DynamoDBEntityFind.list whereCondition: ${whereCondition}")
            AttributeValue hashAttrVal = whereCondition.getDynamoDBHashValue(ed)
            logger.info("DynamoDBEntityFind.list , hashAttrVal: ${hashAttrVal.toString()}")
            String hashFieldName = ed.getFieldNames(true, false, false)[0]
            QuerySpec querySpec = new QuerySpec().withHashKey(hashFieldName, hashAttrVal.getS())
            RangeKeyCondition rangeCondition = whereCondition.getRangeCondition(ed)
            logger.info("DynamoDBFindEntity(170), rangeCondition: ${rangeCondition}")
            if (rangeCondition) {
                querySpec = querySpec.withRangeKeyCondition(rangeCondition)
            }

            String entName = ed.getFullEntityName()
            logger.info("DynamoDBEntityFind.one entName: ${entName}")
            Table table = dynamoDB.getTable(entName)
            logger.info("DynamoDBEntityFind.one table: ${table}")
            ItemCollection <QueryOutcome> queryOutcomeList = table.query(querySpec)
            //List <Item> itemList = queryOutcome.getItems()
            
            logger.info("DynamoDBEntityFind.list queryOutcomeList: ${queryOutcomeList}")
            Map<java.lang.String,java.lang.Object> itemAsMap
            queryOutcomeList.each() {item ->
                itemAsMap = item.asMap()
                logger.info("DynamoDBEntityFind.list itemAsMap: ${itemAsMap}")
                entValue = ddf.makeEntityValue(entName) 
                //entValue.buildEntityValueMap()
                entValue.setAll(itemAsMap)
                entList.add(entValue)
            }
            
            
            
        } catch(ProvisionedThroughputExceededException e1) {
            throw new EntityException(e1.getMessage())
        } catch(ConditionalCheckFailedException e2) {
            throw new EntityException(e2.getMessage())
        } catch(InternalServerErrorException e3) {
            throw new EntityException(e3.getMessage())
        } catch(ResourceNotFoundException e4) {
            throw new EntityException(e4.getMessage())
        } catch(AmazonClientException e5) {
            throw new EntityException(e5.getMessage())
        } catch(AmazonServiceException e6) {
            throw new EntityException(e6.getMessage())
        }finally {
        }
        return entList
    }

    long countExtended(DynamoDBEntityConditionImplBase whereCondition, DynamoDBEntityConditionImplBase havingCondition)
            throws EntityException {
        EntityDefinition ed = this.getEntityDef()
        EntityFindBuilder efb = new EntityFindBuilder(ed, this)

        // count function instead of select fields
        efb.makeCountFunction()
        // FROM Clause
        efb.makeSqlFromClause()

        // WHERE clause
        if (whereCondition) {
            efb.startWhereClause()
            whereCondition.makeSqlWhere(efb)
        }
        // GROUP BY clause
        efb.makeGroupByClause(this.fieldsToSelect)
        // HAVING clause
        if (havingCondition) {
            efb.startHavingClause()
            havingCondition.makeSqlWhere(efb)
        }

        efb.closeCountFunctionIfGroupBy()

        // run the SQL now that it is built
        long count = 0
        try {
            efi.getEntityDbMeta().checkTableRuntime(ed)
            count = internalCount(efb)
        } catch (SQLException e) {
            throw new EntityException("Error finding count", e)
        } finally {
            efb.closeAll()
        }

        return count
    }

    protected long internalCount(EntityFindBuilder efb) {
        long count = 0
        efb.makeConnection()
        efb.makePreparedStatement()
        efb.setPreparedStatementValues()

        ResultSet rs = efb.executeQuery()
        if (rs.next()) count = rs.getLong(1)
        return count
    }
}
