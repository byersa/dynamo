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
import org.apache.commons.collections.set.ListOrderedSet

import org.moqui.entity.EntityException
import org.moqui.impl.entity.EntityDefinition
import org.moqui.impl.entity.EntityFacadeImpl
import org.moqui.impl.entity.EntityValueBase
import org.moqui.impl.entity.EntityValueImpl
import org.moqui.entity.EntityValue
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

import com.amazonaws.services.dynamodb.model.ProvisionedThroughputExceededException
import com.amazonaws.services.dynamodb.model.ConditionalCheckFailedException
import com.amazonaws.services.dynamodb.model.InternalServerErrorException
import com.amazonaws.services.dynamodb.model.ResourceNotFoundException
import com.amazonaws.AmazonClientException
import com.amazonaws.AmazonServiceException

import org.moqui.impl.entity.dynamodb.DynamoDBDatasourceFactory


class DynamoDBEntityValue extends EntityValueBase {
    protected final static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DynamoDBEntityValue.class)

    DynamoDBDatasourceFactory ddf

    DynamoDBEntityValue(EntityDefinition ed, EntityFacadeImpl efip, DynamoDBDatasourceFactory ddf) {
        super(ed, efip)
        this.ddf = ddf
    }

    DynamoDBEntityValue(EntityDefinition ed, EntityFacadeImpl efip, DynamoDBDatasourceFactory ddf, Map document) {
        super(ed, efip)
        this.ddf = ddf
        for (String fieldName in ed.getAllFieldNames()) {
            getValueMap().put(fieldName, document.field(ed.getColumnName(fieldName, false)))
        }
    }

    @Override
    void createExtended(ListOrderedSet fieldList) {
    
        logger.info("In DynamoDBEntityValue, fieldList: ${fieldList}")
        EntityDefinition ed = getEntityDefinition()
        if (ed.isViewEntity()) throw new EntityException("Create not yet implemented for view-entity")

        AmazonDynamoDBClient client = ddf.getDatabase()
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>()
        try {
        logger.info("In DynamoDBEntityValue, valueMap: ${this.getValueMap()}")
            this.buildAttributeValueMap(item, this.getValueMap());
        logger.info("In DynamoDBEntityValue, item: ${item}")
            PutItemRequest putItemRequest = new PutItemRequest().withTableName(ed.getEntityName()).withItem(item);
            PutItemResult result = client.putItem(putItemRequest)     
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
    void updateExtended(List<String> pkFieldList, ListOrderedSet nonPkFieldList) {
        EntityDefinition ed = getEntityDefinition()
        if (ed.isViewEntity()) throw new EntityException("Update not yet implemented for view-entity")

        AmazonDynamoDBClient client = ddf.getDatabase()
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>()
        try {
            buildAttributeValueMap(item, valueMap);
            String entName = ed.getEntityName()
            Key key = new Key()
            Map<String, Object>primaryKeyMap = getPrimaryKeys()
            if (primaryKeyMap && primaryKeyMap.keySet().size()) {
                for(String primaryKeyName in primaryKeyMap) {
                    AttributeValue keyAttributeValue = getAttributeValue(primaryKeyName)
                    key.setHashKeyElement(keyAttributeValue)
                    break;
                }
            } else {
                throw new EntityException("Entity '${entName}' does not have a primary key defined.")
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
            UpdateItemRequest updateItemRequest = new UpdateItemRequest().withTableName(entName).withKey(key).withItem(item);
            UpdateItemResult result = client.updateItem(updateItemRequest)     
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
    void deleteExtended() {
        EntityDefinition ed = getEntityDefinition()
        if (ed.isViewEntity()) throw new EntityException("Delete not implemented for view-entity")

        AmazonDynamoDBClient client = ddf.getDatabase()
        try {
            String entName = ed.getEntityName()
            Key key = new Key()
            Map<String, Object>primaryKeyMap = getPrimaryKeys()
            if (primaryKeyMap && primaryKeyMap.keySet().size()) {
                for(String primaryKeyName in primaryKeyMap) {
                    AttributeValue keyAttributeValue = getAttributeValue(primaryKeyName)
                    key.setHashKeyElement(keyAttributeValue)
                    break;
                }
            } else {
                throw new EntityException("Entity '${entName}' does not have a primary key defined.")
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
            DeleteItemRequest deleteItemRequest = new DeleteItemRequest().withTableName(entName).withKey(key);
            DeleteItemResult result = client.updateItem(deleteItemRequest)     
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
        EntityDefinition ed = getEntityDefinition()

        AmazonDynamoDBClient client = ddf.getDatabase()
        try {
            String entName = ed.getEntityName()
            Key key = new Key()
            Map<String, Object>primaryKeyMap = getPrimaryKeys()
            if (primaryKeyMap && primaryKeyMap.keySet().size()) {
                for(String primaryKeyName in primaryKeyMap) {
                    AttributeValue keyAttributeValue = getAttributeValue(primaryKeyName)
                    key.setHashKeyElement(keyAttributeValue)
                    break;
                }
            } else {
                throw new EntityException("Entity '${entName}' does not have a primary key defined.")
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
            GetItemRequest getItemRequest = new GetItemRequest().withTableName(entName).withKey(key).withItem(item);
            GetItemResult result = client.getItem(getItemRequest)     
            
            java.util.Map<java.lang.String,AttributeValue> returnAttributeValueMap = result.getItem()
            buildEntityValueMap(returnAttributeValueMap)
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
        EntityDefinition ed = this.getEntityDefinition()
        ListOrderedSet fieldNames = ed.getFieldNames(true, true, true)
        for(String fieldName in fieldNames) {
            AttributeValue attrVal = getAttributeValue(fieldName)
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
    
    void buildEntityValueMap( Map<String, EntityValue> attributeValueItem) {
    
        for(String fieldName in attributeValueItem) {
            Node fieldNode = this.getEntityDefinition().getFieldNode(fieldName)
            switch(fieldNode."@type") {
                case "id":
                case "id-long":
                case "text-short":
                case "text-medium":
                case "text-long":
                case "text-very-long":
                case "text-indicator":
                     this.set(fieldName, attributeValueItem[fieldName].getS())
                case "number-integer":
                case "number-decimal":
                case "number-float":
                case "currency-amount":
                case "currency-precise":
                case "time":
                     this.set(fieldName, attributeValueItem[fieldName].getN())
                case "date":
                     tm = attributeValueItem[fieldName].getN()
                     this.set(fieldName,  new Date(tm))
                case "date-time":
                     tm = attributeValueItem[fieldName].getN()
                     this.set(fieldName, new Timestamp(tm))
                default:
                     this.set(fieldName, null)
            }
        }
        
    }
    
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
    
    
}
