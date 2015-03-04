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

import java.sql.Timestamp
import java.sql.Date
import java.text.SimpleDateFormat
import org.apache.commons.collections.set.ListOrderedSet

import org.moqui.entity.EntityException
import org.moqui.impl.entity.EntityDefinition
import org.moqui.impl.entity.EntityFacadeImpl
import org.moqui.impl.entity.EntityValueBase
import org.moqui.impl.entity.EntityValueImpl
import org.moqui.entity.EntityValue
import org.moqui.entity.EntityFind
import org.moqui.impl.entity.dynamodb.DynamoDBEntityConditionFactoryImpl
import org.moqui.impl.entity.dynamodb.condition.DynamoDBEntityConditionImplBase
import org.moqui.impl.entity.dynamodb.DynamoDBDatasourceFactory
import org.moqui.impl.entity.dynamodb.DynamoDBEntityFind
import org.moqui.impl.entity.dynamodb.DynamoDBUtils

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
import com.amazonaws.services.dynamodbv2.document.spec.PutItemSpec
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome
import com.amazonaws.services.dynamodbv2.document.Item
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec
import com.amazonaws.services.dynamodbv2.document.QueryOutcome
import com.amazonaws.services.dynamodbv2.document.RangeKeyCondition

import java.sql.Connection

import org.moqui.impl.entity.dynamodb.DynamoDBDatasourceFactory


class DynamoDBEntityValue extends EntityValueBase {
    protected final static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DynamoDBEntityValue.class)

    protected DynamoDBDatasourceFactory ddf
    protected DynamoDBEntityConditionFactoryImpl conditionFactory 

    DynamoDBEntityValue(EntityDefinition ed, EntityFacadeImpl efip, DynamoDBDatasourceFactory ddf) {
        super(ed, efip)
        this.conditionFactory = new DynamoDBEntityConditionFactoryImpl(efip)
        this.ddf = ddf
    }

    DynamoDBEntityValue(EntityDefinition ed, EntityFacadeImpl efip, DynamoDBDatasourceFactory ddf, Map valMap) {
        super(ed, efip)
        this.ddf = ddf
        for (String fieldName in ed.getAllFieldNames()) {
            Map<String, AttributeValue> valueMap = this.getValueMap()
            valueMap.put(fieldName, valMap.field(ed.getColumnName(fieldName, false)))
        }
    }

    @Override
    void createExtended(ListOrderedSet fieldList, Connection con) {
    
        EntityDefinition entityDefinition = getEntityDefinition()
        logger.info("In DynamoDBEntityValue.create, fieldList: ${fieldList}")
        if (entityDefinition.isViewEntity()) throw new EntityException("Create not yet implemented for view-entity")

        AmazonDynamoDBClient client = ddf.getDynamoDBClient()
        DynamoDB dynamoDB = ddf.getDatabase()
        try {
            String tableName = entityDefinition.getFullEntityName()
            Table table = dynamoDB.getTable(tableName)
            logger.info("In DynamoDBEntityValue.create, table: ${table}")
            Map<String, Object> valueMap = this.getValueMap()
            logger.info("In DynamoDBEntityValue.create, valueMap: ${valueMap}")
            Item item = Item.fromMap(valueMap)
            PutItemSpec putItemSpec = new PutItemSpec().withItem(item)
            PutItemOutcome putItemOutcome = table.putItem(putItemSpec)
            logger.info("In DynamoDBEntityValue.create, putItemOutcome: ${putItemOutcome}")
            //this.buildAttributeValueMap(item, valueMap);
            //logger.info("In DynamoDBEntityValue.create, item: ${item}")
            //PutItemRequest putItemRequest = new PutItemRequest().withTableName(entityDefinition.getFullEntityName()).withItem(item);
            //PutItemResult result = client.putItem(putItemRequest)     
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
    }

    @Override
    void updateExtended(List<String> pkFieldList, ListOrderedSet nonPkFieldList, Connection con) {
    
        List <String> fieldList = new ArrayList()
        ListOrderedSet newLOS = new ListOrderedSet(nonPkFieldList)
        newLOS.addAll(pkFieldList)
        logger.info("DynamoDBEntityValue.updateExtended, newLOS: ${newLOS}")
        this.createExtended(newLOS, con)
//        DynamoDBEntityValue entValue = null
//        logger.info("DynamoDBEntityValue.updateExtended (111), this: ${this.toString()}")
//        EntityDefinition ed = getEntityDefinition() //        if (ed.isViewEntity()) throw new EntityException("Update not yet implemented for view-entity")
//
//            Map<String, AttributeValue> valueMap = this.getValueMap()
//        logger.info("DynamoDBEntityValue.updateExtended, valueMap: ${valueMap}")
//        DynamoDBEntityConditionImplBase whereCondition
//        //if (ed.containsPrimaryKey(valueMap)) {
//            whereCondition = (DynamoDBEntityConditionImplBase) this.conditionFactory.makeCondition(valueMap)
//        logger.info("DynamoDBEntityValue.updateExtended, whereCondition: ${whereCondition}")
//        //} else {
//            //throw(new EntityException("In update, primary key not contained in ${valueMap}"))
//        //}
//
//        try {
//            String entName = ed.getFullEntityName()
//            logger.info("DynamoDBEntityValue (updateExtended), entName: ${entName}")
//            EntityFind entityFind = efi.makeFind(entityName)
//            logger.info("DynamoDBEntityValue (updateExtended), entityFind: ${entityFind}")
//            EntityValue newValue = entityFind.condition(whereCondition).one()
//            logger.info("DynamoDBEntityValue (updateExtended), newValue: ${newValue}")
////            EntityValue newValue = entityFind.condition(this.getValueMap()).one()
////            AttributeValue attrVal = whereCondition.getDynamoDBHashValue(ed)
////            logger.info("DynamoDBEntityValue (updateExtended), attrVal: ${attrVal.toString()}")
////            String hashFieldName = ed.getFieldNames(true, false, false)[0]
////            Map<String, AttributeValue> keyConditions = new  HashMap()
////            keyConditions.put(hashFieldName, attrVal)
////            Map<String, AttributeValue> key = new HashMap()
////            if (attrVal) {
////                key.setHashKeyElement(attrVal)
////            } else {
////                throw(new EntityException("In update, the condition ${whereCondition} for the entity: ${entName} does not specify a value for the primary key."))
////            }
////            AttributeValue attrVal2 = whereCondition.getDynamoDBRangeValue(entityDefinition)
////            // TODO: check to see if entity requires a range value to define the primary key
////            if (attrVal2) {
////                key.setRangeKeyElement(attrVal2)
////            }
////        AmazonDynamoDBClient client = ddf.getDynamoDBClient()
////        Map<String, AttributeValueUpdate> item = new HashMap<String, AttributeValueUpdate>()
////        logger.info("In DynamoDBEntityValue.update, valueMap: ${this.getValueMap()}")
////            this.buildAttributeValueUpdateMap(item, this.getValueMap());
////        logger.info("In DynamoDBEntityValue.update, item: ${item}")
////            UpdateItemRequest updateItemRequest = new UpdateItemRequest().withTableName(entName).withKey(key).withAttributeUpdates(item);
////            UpdateItemResult result = client.updateItem(updateItemRequest)     
//        } catch(ProvisionedThroughputExceededException e1) {
//            throw new EntityException(e1.getMessage())
//        } catch(ConditionalCheckFailedException e2) {
//            throw new EntityException(e2.getMessage())
//        } catch(InternalServerErrorException e3) {
//            throw new EntityException(e3.getMessage())
//        } catch(ResourceNotFoundException e4) {
//            throw new EntityException(e4.getMessage())
//        } catch(AmazonClientException e5) {
//            throw new EntityException(e5.getMessage())
//        } catch(AmazonServiceException e6) {
//            throw new EntityException(e6.getMessage())
//        }finally {
//        }
    }

    @Override
    void deleteExtended(Connection con) {
        EntityDefinition entityDefinition = getEntityDefinition()
        if (entityDefinition.isViewEntity()) throw new EntityException("Update not yet implemented for view-entity")

            Map<String, AttributeValue> valueMap = this.getValueMap()
        logger.info("DynamoDBEntityFind.one (73), simpleAndMap: ${valueMap}")
        DynamoDBEntityConditionImplBase whereCondition
        if (entityDefinition.containsPrimaryKey(valueMap)) {
            whereCondition = (DynamoDBEntityConditionImplBase) this.conditionFactory.makeCondition(valueMap)
        } else {
            throw(new EntityException("In update, primary key not contained in ${valueMap}"))
        }


        DynamoDBEntityValue entValue = null
        try {
            String entName = entityDefinition.getFullEntityName()
            Map<String, AttributeValue> key = new HashMap()
            AttributeValue attrVal = whereCondition.getDynamoDBHashValue(entityDefinition)
            if (attrVal) {
                //key.setHashKeyElement(attrVal)
            } else {
                throw(new EntityException("In update, the condition ${whereCondition} for the entity: ${entName} does not specify a value for the primary key."))
            }
            AttributeValue attrVal2 = whereCondition.getDynamoDBRangeValue(entityDefinition)
            // TODO: check to see if entity requires a range value to define the primary key
            if (attrVal2) {
                //key.setRangeKeyElement(attrVal2)
            }
            AmazonDynamoDBClient client = ddf.getDatabase()
            DeleteItemRequest deleteItemRequest = new DeleteItemRequest().withTableName(entName).withKey(key);
            DeleteItemResult result = client.deleteItem(deleteItemRequest)     
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
    }

    @Override
    boolean refreshExtended() {

        AmazonDynamoDBClient client = ddf.getDatabase()
        try {
            EntityFind entityFind = efi.makeFind(entityName)
            EntityValue newValue = entityFind.condition(this.getValueMap()).one()
            this.setAll(newValue)
            return !!newValue
            /*
            Key key = new Key()
            
            Map<String, Object>primaryKeyMap = getPrimaryKeys()
            if (primaryKeyMap && primaryKeyMap.keySet().size()) {
                for(String primaryKeyName in primaryKeyMap) {
                    AttributeValue keyAttributeValue = getAttributeValue(primaryKeyName)
                    key.setHashKeyElement(keyAttributeValue)
                    break;
                }
            } else {
                throw new EntityException("Entity '${entityName}' does not have a primary key defined.")
            }
            
            // see if there is a range key defined as a field with the index defined
            List<Node> fieldNodes = getFieldNodes(false, true, false)
            for (Node nd in fieldNodes) {
                if (nd."@index") {
                    AttributeValue keyAttributeValue = getAttributeValue(nd."@field")
                    key.setRangeKeyElement(keyAttributeValue)
                    break;
                }
            }
            GetItemRequest getItemRequest = new GetItemRequest().withTableName(entityName).withKey(key).withItem(item);
            GetItemResult result = client.getItem(getItemRequest)     
            
            java.util.Map<java.lang.String,AttributeValue> returnAttributeValueMap = result.getItem()
            buildEntityValueMap(returnAttributeValueMap)
            */
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
    }
    
    void buildAttributeValueMap( Map<String, AttributeValue> item, Map<String, Object> valueMap) {
        EntityDefinition entityDefinition = getEntityDefinition()
        ListOrderedSet fieldNames = entityDefinition.getFieldNames(true, true, true)
        for(String fieldName in fieldNames) {
            AttributeValue attrVal = DynamoDBUtils.getAttributeValue(fieldName, valueMap, entityDefinition)
                 logger.info("DynamoDBEntityValue.buildAttributeValueMap(250) attrVal: ${attrVal}")
            if (attrVal != null) {
                 logger.info("DynamoDBEntityValue.buildAttributeValueMap(252) fieldName: ${fieldName}, attrVal: ${attrVal}")
                item.put(fieldName, attrVal)
            } else {
                 logger.info("DynamoDBEntityValue.buildAttributeValueMap(remove - 255) fieldName: ${fieldName}")
                item.remove(fieldName)
            }
        }
        
    }
    
    void buildAttributeValueUpdateMap( Map<String, AttributeValueUpdate> item, Map<String, Object> valueMap) {
        EntityDefinition entityDefinition = getEntityDefinition()
        ListOrderedSet fieldNames = entityDefinition.getFieldNames(true, true, true)
        for(String fieldName in fieldNames) {
            AttributeValueUpdate attrVal = DynamoDBUtils.getAttributeValueUpdate(fieldName, valueMap, entityDefinition)
                 logger.info("DynamoDBEntityValue.buildAttributeValueMap(250) attrVal: ${attrVal}")
            if (attrVal != null) {
                 logger.info("DynamoDBEntityValue.buildAttributeValueMap(252) fieldName: ${fieldName}, attrVal: ${attrVal}")
                item.put(fieldName, attrVal)
            } else {
                 logger.info("DynamoDBEntityValue.buildAttributeValueMap(remove - 255) fieldName: ${fieldName}")
                item.remove(fieldName)
            }
        }
        
    }
    
    void testFunction() {
        return;
    }

    //void buildEntityValueMap( Map<String, AttributeValue> attributeValueItem) {
    void buildEntityValueMap( ) {
     return;
    
        String fieldName, fieldType
        AttributeValue attrVal
        def tm, num
        for(Map.Entry fieldEntry in attributeValueItem) {
            fieldName = fieldEntry.key
                 logger.info("DynamoDBEntityValue.buildEntityValueMap(280) fieldName: ${fieldName}")
            Node fieldNode = this.getEntityDefinition().getFieldNode(fieldName)
            fieldType = fieldNode."@type"
                 logger.info("DynamoDBEntityValue.buildEntityValueMap(282) type: ${fieldType}")
            switch(fieldType) {
                case "id":
                case "id-long":
                case "text-short":
                case "text-medium":
                case "text-long":
                case "text-very-long":
                case "text-indicator":
                     this.set(fieldName, attributeValueItem[fieldName].getS())
                     break
                case "number-integer":
                case "number-decimal":
                case "number-float":
                case "currency-amount":
                case "currency-precise":
                case "time":
                     attrVal = attributeValueItem[fieldName]
                     num = attrVal.getN()
                     this.set(fieldName, Long.parseLong(num))
                     break
                case "date":
                     attrVal = attributeValueItem[fieldName]
                 logger.info("DynamoDBEntityValue.buildEntityValueMap(300) attrVal: ${attrVal}")
                     tm = attrVal.getN()
                     Date dt = new Date(tm)
                 logger.info("DynamoDBEntityValue.buildEntityValueMap(303) tm: ${tm}, dt: ${dt}")
                     this.set(fieldName, dt )
                     break
                case "date-time":
                     attrVal = attributeValueItem[fieldName]
                 logger.info("DynamoDBEntityValue.buildEntityValueMap(313) attrVal: ${attrVal}")
                     tm = attrVal.getS()
                 logger.info("DynamoDBEntityValue.buildEntityValueMap(315) tm: ${tm}")
                     Timestamp ts = Timestamp.valueOf(tm)
                 logger.info("DynamoDBEntityValue.buildEntityValueMap(317) ts: ${ts}")
                     this.set(fieldName, ts )
                     break
                default:
                     this.set(fieldName, null)
            }
        }
        
    }

    EntityValue cloneValue() {
        // FIXME
        return this
    }

    Map<String, Object> getValueMap() {
        Map<String, Object> newValueMap = new HashMap()
        Map<String, Object> parentValueMap = super.getValueMap()
        logger.info("parentValueMap: ${parentValueMap}")
        parentValueMap.each{k,v ->
            if (v instanceof Timestamp) {
               logger.info("${k} is Timestamp")
               newValueMap[k] = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(v)
            } else {
               newValueMap[k] = v 
            }   
        }   
        logger.info("newValueMap: ${newValueMap}")
        return newValueMap
    }
/*
    AttributeValue getAttributeValue(fieldName) {
    
        AttributeValue attrVal = new AttributeValue()
        Node fieldNode = this.getEntityDefinition().getFieldNode(fieldName)
        String fieldNodeName = fieldNode."@name"
                 logger.info("DynamoDBEntityValue.getAttributeValue(291) fieldNodeName: ${fieldNodeName}")
        String fieldNodeType = fieldNode."@type"
                 logger.info("DynamoDBEntityValue.getAttributeValue(293) fieldNodeType: ${fieldNodeType}")
        switch(fieldNodeType) {
            case "id":
            case "id-long":
            case "text-short":
            case "text-medium":
            case "text-long":
            case "text-very-long":
            case "text-indicator":
                 String val = this.get(fieldName)?: ""
                 logger.info("DynamoDBEntityValue.getAttributeValue(311) val: ${val}")
                 if (val) {
                    attrVal.setS(val)
                 } else {
                     return null
                 }
                 break
            case "number-integer":
            case "number-decimal":
            case "number-float":
            case "currency-amount":
            case "currency-precise":
                 return attrVal.setN(this.get(fieldName).toString())
                 break
            case "date":
            case "time":
            case "date-time":
                 String dateTimeStr = this.get(fieldName)
                 logger.info("DynamoDBEntityValue(310) dateTimeStr: ${dateTimeStr}")
                 Timestamp ts = Timestamp.valueOf(dateTimeStr)
                 logger.info("DynamoDBEntityValue(312) ts: ${ts}")
                 attrVal.setN(ts.getTime().toString())
                 break
            default:
                 String val = this.get(fieldName)?: ""
                 if (val) {
                     attrVal.setS(val)
                 } else {
                     return null
                 }
        }
        
        return attrVal
    }
*/    
    

}
