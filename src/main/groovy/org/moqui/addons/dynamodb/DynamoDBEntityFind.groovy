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

import com.amazonaws.services.dynamodb.AmazonDynamoDBClient
import com.amazonaws.services.dynamodb.model.AttributeAction
import com.amazonaws.services.dynamodb.model.AttributeValue
import com.amazonaws.services.dynamodb.model.AttributeValueUpdate
import com.amazonaws.services.dynamodb.model.ConditionalCheckFailedException
import com.amazonaws.services.dynamodb.model.DeleteItemRequest
import com.amazonaws.services.dynamodb.model.DeleteItemResult
import com.amazonaws.services.dynamodb.model.ExpectedAttributeValue
import com.amazonaws.services.dynamodb.model.GetItemRequest
import com.amazonaws.services.dynamodb.model.GetItemResult
import com.amazonaws.services.dynamodb.model.Key
import com.amazonaws.services.dynamodb.model.PutItemRequest
import com.amazonaws.services.dynamodb.model.PutItemResult
import com.amazonaws.services.dynamodb.model.ReturnValue
import com.amazonaws.services.dynamodb.model.UpdateItemRequest
import com.amazonaws.services.dynamodb.model.UpdateItemResult
import com.amazonaws.services.dynamodb.model.QueryRequest
import com.amazonaws.services.dynamodb.model.QueryResult
import com.amazonaws.services.dynamodb.model.Condition

import com.amazonaws.services.dynamodb.model.ProvisionedThroughputExceededException
import com.amazonaws.services.dynamodb.model.ConditionalCheckFailedException
import com.amazonaws.services.dynamodb.model.InternalServerErrorException
import com.amazonaws.services.dynamodb.model.ResourceNotFoundException
import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonServiceException


class DynamoDBEntityFind extends DynamoDBEntityFindBase {
    protected final static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DynamoDBEntityFind.class)
    protected AmazonDynamoDBClient client
    protected DynamoDBDatasourceFactory ddf

    DynamoDBEntityFind(EntityFacadeImpl efi, String entityName, DynamoDBDatasourceFactory ddf) {
        super(efi, entityName)
        this.ddf = ddf
        this.client = ddf.getDatabase()
    }

    // ======================== Run Find Methods ==============================

    EntityValue one() throws EntityException {

        EntityDefinition ed = this.getEntityDef()
        //Map<String,Object> valMap = getValueMap()
        // if over-constrained (anything in addition to a full PK), just use the full PK
        logger.info("DynamoDBEntityFind.one (73), simpleAndMap: ${simpleAndMap}")
        EntityCondition whereCondition = this.getWhereEntityCondition()
        /*
        if (ed.containsPrimaryKey(simpleAndMap)) {
            whereCondition = (DynamoDBEntityConditionImplBase) this.conditionFactory.makeCondition(simpleAndMap)
        } else {
            throw(new EntityException("Primary key not contained in ${simpleAndMap}"))
        }
        */


        DynamoDBEntityValue entValue = null
        try {
            String entName = ed.getEntityName()
            Key key = new Key()
logger.info("DynamoDBFindEntity(96), whereCondition: ${whereCondition.toString()}")
            AttributeValue attrVal = whereCondition.getDynamoDBHashValue(ed)
            if (attrVal) {
                key.setHashKeyElement(attrVal)
            } else {
                throw(new EntityException("The condition ${whereCondition} for the entity: ${entName} does not specify a value for the primary key."))
            }
            AttributeValue attrVal2 = whereCondition.getDynamoDBRangeValue(ed)
            // TODO: check to see if entity requires a range value to define the primary key
            if (attrVal2) {
                key.setRangeKeyElement(attrVal2)
            }
        logger.info("DynamoDBEntityFind.one (101), key: ${key.toString()}")
            GetItemRequest getItemRequest = new GetItemRequest().withTableName(entName).withKey(key);
            GetItemResult result = client.getItem(getItemRequest)     
                
            java.util.Map<java.lang.String,AttributeValue> returnAttributeValueMap = result.getItem()
            
        logger.info("DynamoDBEntityFind.one (106), returnAttributeValueMap: ${returnAttributeValueMap}")
        if (returnAttributeValueMap) {
            entValue = efi.makeValue(entName) 
            entValue.buildEntityValueMap(returnAttributeValueMap)
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

    @Override
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
        EntityList <DynamoDBEntityValue> entList = new EntityListImpl(this.efi)
        try {
            DynamoDBEntityConditionImplBase whereCondition = this.getWhereEntityCondition()
            String entName = ed.getEntityName()
logger.info("DynamoDBFindEntity(157), WHERECONDITION: ${whereCondition}")
            AttributeValue attrVal = whereCondition.getDynamoDBHashValue(ed)
            if (attrVal) {
//                key.setHashKeyElement(attrVal)
            } else {
                throw(new EntityException("The condition ${whereCondition} for the entity: ${entName} does not specify a value for the primary key."))
            }
            
        QueryRequest queryRequest = new QueryRequest().withTableName(entName)
            .withHashKeyValue(attrVal)
            
            
            Condition rangeCondition = whereCondition.getDynamoDBRangeCondition(ed)
logger.info("DynamoDBFindEntity(170), rangeCondition: ${rangeCondition}")
            if (rangeCondition) {
                queryRequest.setRangeKeyCondition(rangeCondition)
            }

logger.info("DynamoDBFindEntity(175), queryRequest: ${queryRequest}")
        QueryResult result = client.query(queryRequest);
logger.info("DynamoDBFindEntity(176), result: ${result.getItems()}")
        DynamoDBEntityValue entValue = null
        for (Map<String, AttributeValue> item : result.getItems()) {
            entValue = new DynamoDBEntityValue(ed, this.efi, this.ddf)
            entValue.buildEntityValueMap(item)
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

    @Override
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
