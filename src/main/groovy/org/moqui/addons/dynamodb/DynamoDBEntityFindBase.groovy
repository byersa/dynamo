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

import org.moqui.entity.EntityFind
import org.moqui.impl.entity.condition.EntityConditionImplBase
import org.apache.commons.collections.set.ListOrderedSet
import java.sql.ResultSet
import org.moqui.entity.EntityDynamicView
import org.moqui.entity.EntityCondition
import org.moqui.impl.entity.condition.ListCondition
import org.moqui.context.ExecutionContext
import java.sql.Timestamp
import org.moqui.entity.EntityValue
import org.moqui.impl.entity.EntityFacadeImpl
import org.moqui.impl.entity.EntityDefinition
import org.moqui.impl.entity.EntityDynamicViewImpl
import org.moqui.entity.EntityException
import org.moqui.entity.EntityList
import org.moqui.entity.EntityListIterator
import org.moqui.impl.context.ArtifactExecutionInfoImpl
import org.moqui.impl.context.CacheImpl
import net.sf.ehcache.Element
import org.moqui.impl.entity.dynamodb.condition.DynamoDBEntityConditionImplBase
import org.moqui.impl.entity.dynamodb.DynamoDBEntityConditionFactoryImpl

class DynamoDBEntityFindBase implements EntityFind {
    protected final static org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DynamoDBEntityFindBase.class)

    protected final EntityFacadeImpl efi

    protected String entityName
    protected EntityDefinition entityDef = null
    protected EntityDynamicViewImpl dynamicView = null

    protected Map<String, Object> simpleAndMap = null
    protected DynamoDBEntityConditionImplBase whereEntityCondition = null
    protected DynamoDBEntityConditionImplBase havingEntityCondition = null

    /** This is always a ListOrderedSet so that we can get the results in a consistent order */
    protected ListOrderedSet fieldsToSelect = null
    protected List<String> orderByFields = null

    protected Boolean useCache = null

    protected boolean distinct = false
    protected Integer offset = null
    protected Integer limit = null
    protected boolean forUpdate = false

    protected int resultSetType = ResultSet.TYPE_SCROLL_INSENSITIVE
    protected int resultSetConcurrency = ResultSet.CONCUR_READ_ONLY
    protected Integer fetchSize = null
    protected Integer maxRows = null
    protected DynamoDBEntityConditionFactoryImpl conditionFactory 

    DynamoDBEntityFindBase(EntityFacadeImpl efi, String entityName) {
        this.efi = efi
        this.entityName = entityName
        this.conditionFactory = new DynamoDBEntityConditionFactoryImpl(efi)
    }

    /** @see org.moqui.entity.EntityFind#entity(String) */
    EntityFind entity(String entityName) { this.entityName = entityName; return this }

    /** @see org.moqui.entity.EntityFind#getEntity() */
    String getEntity() { return this.entityName }

    // ======================== Conditions (Where and Having) =================

    /** @see org.moqui.entity.EntityFind#condition(String, Object) */
    EntityFind condition(String fieldName, Object value) {
        if (!this.simpleAndMap) this.simpleAndMap = new HashMap()
    logger.info("DynamoDBEntityFindBase(83), condition, fieldName: ${fieldName}, value: ${value}")
        this.simpleAndMap.put(fieldName, value)
        logger.info("DynamoDBEntityFindBase(85), condition, simplAndMap: ${simpleAndMap}")
        return this
    }

    EntityFind condition(String fieldName, EntityCondition.ComparisonOperator operator, Object value) {
    logger.info("DynamoDBEntityFindBase(88), condition, fieldName: ${fieldName}, operator: ${operator}, value: ${value}")
        condition(conditionFactory.makeCondition(fieldName, operator, value))
        return this
    }

    EntityFind conditionToField(String fieldName, EntityCondition.ComparisonOperator operator, String toFieldName) {
        condition(conditionFactory.makeCondition(fieldName, operator, toFieldName))
        return this
    }

    /** @see org.moqui.entity.EntityFind#condition(Map<String,?>) */
    EntityFind condition(Map<String, ?> fields) {
        if (!fields) return this
        if (!this.simpleAndMap) this.simpleAndMap = new HashMap()
        getEntityDef().setFields(fields, this.simpleAndMap, true, null, null)
    logger.info("DynamoDBEntityFindBase(102), condition, simplAndMap: ${simpleAndMap}")
        return this
    }

    /** @see org.moqui.entity.EntityFind#condition(EntityCondition) */
    EntityFind condition(EntityCondition condition) {
        if (!condition) return this
        whereEntityCondition = this.getWhereEntityCondition()
    logger.info("DynamoDBEntityFindBase(110), condition, whereEntityCondition: ${whereEntityCondition}, condition: ${condition}")
        if (whereEntityCondition) {
            // use ListCondition instead of ANDing two at a time to avoid a bunch of nested ANDs
            if (whereEntityCondition instanceof ListCondition) {
                ((ListCondition) whereEntityCondition).addCondition((DynamoDBEntityConditionImplBase) condition)
            } else {
                whereEntityCondition =
                    (DynamoDBEntityConditionImplBase) conditionFactory.makeCondition([whereEntityCondition, condition])
            }
        } else {
            whereEntityCondition = (DynamoDBEntityConditionImplBase) condition
        }
    logger.info("DynamoDBEntityFindBase(123), condition, whereEntityCondition: ${this.whereEntityCondition}")
        return this
    }

    EntityFind condition(String arg1, String arg2, Object obj3) {
        // FIXME: put here to satisfy non-abstract requirement
        return this
    }
 
    EntityFind disableAuthz() {
        // FIXME: put here to satisfy non-abstract requirement
        return this
    }

    EntityFind conditionDate(String fromFieldName, String thruFieldName, java.sql.Timestamp compareStamp) {
        condition(conditionFactory.makeConditionDate(fromFieldName, thruFieldName, compareStamp))
        return this
    }

    /** @see org.moqui.entity.EntityFind#havingCondition(EntityCondition) */
    EntityFind havingCondition(EntityCondition condition) {
        if (!condition) return this
        if (havingEntityCondition) {
            // use ListCondition instead of ANDing two at a time to avoid a bunch of nested ANDs
            if (havingEntityCondition instanceof ListCondition) {
                ((ListCondition) havingEntityCondition).addCondition((DynamoDBEntityConditionImplBase) condition)
            } else {
                havingEntityCondition =
                    (DynamoDBEntityConditionImplBase) conditionFactory.makeCondition([havingEntityCondition, condition])
            }
        } else {
            havingEntityCondition = (DynamoDBEntityConditionImplBase) condition
        }
        return this
    }

    /** @see org.moqui.entity.EntityFind#getWhereEntityCondition() */
    EntityCondition getWhereEntityCondition() {
        logger.info("DynamoDBEntityFindBase, getWhereEntityCondition(151), simpleAndMap: ${simpleAndMap}, whereEntityCondition: ${this.whereEntityCondition}")
        if (this.simpleAndMap) {
            EntityCondition simpleAndMapCond = this.conditionFactory.makeCondition(this.simpleAndMap)
            if (this.whereEntityCondition) {
                return this.conditionFactory.makeCondition(simpleAndMapCond, EntityCondition.JoinOperator.AND, this.whereEntityCondition)
            } else {
                this.whereEntityCondition = simpleAndMapCond
                this.simpleAndMap = null
                return this.whereEntityCondition
            }
        } else {
            return this.whereEntityCondition
        }
    }

    /** @see org.moqui.entity.EntityFind#getHavingEntityCondition() */
    EntityCondition getHavingEntityCondition() {
        return this.havingEntityCondition
    }

    /** @see org.moqui.entity.EntityFind#searchFormInputs(String,String,boolean) */
    EntityFind searchFormInputs(String inputFieldsMapName, String defaultOrderBy, boolean alwaysPaginate) {
        Map inf = inputFieldsMapName ? (Map) efi.ecfi.executionContext.context[inputFieldsMapName] :
            (efi.ecfi.executionContext.web ? efi.ecfi.executionContext.web.parameters : efi.ecfi.executionContext.context)
        EntityDefinition ed = getEntityDef()

        for (String fn in ed.getAllFieldNames()) {
            // NOTE: do we need to do type conversion here?

            // this will handle text-find
            if (inf.containsKey(fn) || inf.containsKey(fn + "_op")) {
                Object value = inf.get(fn)
                String op = inf.get(fn + "_op") ?: "contains"
                boolean not = (inf.get(fn + "_not") == "Y")
                boolean ic = (inf.get(fn + "_ic") == "Y")

                EntityCondition ec = null
                switch (op) {
                    case "equals":
                        if (value) {
                            ec = conditionFactory.makeCondition(fn,
                                    not ? EntityCondition.NOT_EQUAL : EntityCondition.EQUALS, value)
                            if (ic) ec.ignoreCase()
                        }
                        break;
                    case "like":
                        if (value) {
                            ec = conditionFactory.makeCondition(fn,
                                    not ? EntityCondition.NOT_LIKE : EntityCondition.LIKE, value)
                            if (ic) ec.ignoreCase()
                        }
                        break;
                    case "contains":
                        if (value) {
                            ec = conditionFactory.makeCondition(fn,
                                    not ? EntityCondition.NOT_LIKE : EntityCondition.LIKE, "%${value}%")
                            if (ic) ec.ignoreCase()
                        }
                        break;
                    case "empty":
                        ec = conditionFactory.makeCondition(
                                conditionFactory.makeCondition(fn,
                                        not ? EntityCondition.NOT_EQUAL : EntityCondition.EQUALS, null),
                                not ? EntityCondition.JoinOperator.AND : EntityCondition.JoinOperator.OR,
                                conditionFactory.makeCondition(fn,
                                        not ? EntityCondition.NOT_EQUAL : EntityCondition.EQUALS, ""))
                        break;
                }
                if (ec != null) this.condition(ec)
            } else {
                // these will handle range-find and date-find
                if (inf.get(fn + "_from")) this.condition(conditionFactory.makeCondition(fn,
                        EntityCondition.GREATER_THAN_EQUAL_TO, inf.get(fn + "_from")))
                if (inf.get(fn + "_thru")) this.condition(conditionFactory.makeCondition(fn,
                        EntityCondition.LESS_THAN, inf.get(fn + "_thru")))
            }
        }

        // always look for an orderByField parameter too
        String orderByString = inf.get("orderByField") ?: defaultOrderBy
        this.orderBy(orderByString)

        // look for the pageIndex and optional pageSize parameters
        if (alwaysPaginate || inf.get("pageIndex")) {
            int pageIndex = (inf.get("pageIndex") ?: 0) as int
            int pageSize = (inf.get("pageSize") ?: (this.limit ?: 20)) as int
            offset(pageIndex * pageSize)
            limit(pageSize)
        }

        // if there is a pageNoLimit clear out the limit regardless of other settings
        if (inf.get("pageNoLimit") == "true") {
            this.offset = null
            this.limit = null
        }

        return this
    }

    int getPageIndex() { return offset == null ? 0 : offset/getPageSize() }
    int getPageSize() { return limit ?: 20 }

    EntityFind findNode(Node node) {
        ExecutionContext ec = this.efi.ecfi.executionContext

        this.entity((String) node["@entity-name"])
        if (node["@cache"]) { this.useCache(node["@cache"] == "true") }
        if (node["@for-update"]) this.forUpdate(node["@for-update"] == "true")
        if (node["@distinct"]) this.distinct(node["@distinct"] == "true")
        if (node["@offset"]) this.offset(node["@offset"] as Integer)
        if (node["@limit"]) this.limit(node["@limit"] as Integer)
        for (Node sf in node["select-field"]) this.selectField((String) sf["@field-name"])
        for (Node ob in node["order-by"]) this.orderBy((String) ob["@field-name"])

        if (!this.getUseCache()) {
            for (Node df in node["date-filter"])
                this.condition(ec.entity.conditionFactory.makeConditionDate((String) df["@from-field-name"] ?: "fromDate",
                        (String) df["@thru-field-name"] ?: "thruDate",
                        (df["@valid-date"] ? ec.resource.evaluateContextField((String) df["@valid-date"], null) as Timestamp : ec.user.nowTimestamp)))
        }

        for (Node ecn in node["econdition"])
            this.condition(((DynamoDBEntityConditionFactoryImpl) conditionFactory).makeActionCondition(ecn))
        for (Node ecs in node["econditions"])
            this.condition(((DynamoDBEntityConditionFactoryImpl) conditionFactory).makeActionConditions(ecs))
        for (Node eco in node["econdition-object"])
            this.condition((EntityCondition) ec.resource.evaluateContextField((String) eco["@field"], null))

        if (node["search-form-inputs"]) {
            Node sfiNode = (Node) node["search-form-inputs"][0]
            searchFormInputs((String) sfiNode["@input-fields-map"], (String) sfiNode["@default-order-by"], (sfiNode["@paginate"] ?: "true") as boolean)
        }
        if (node["having-econditions"]) {
            for (Node havingCond in node["having-econditions"])
                this.havingCondition(conditionFactory.makeActionCondition(havingCond))
        }

        // logger.info("TOREMOVE Added findNode\n${node}\n${this.toString()}")

        return this
    }

    // ======================== General/Common Options ========================

    /** @see org.moqui.entity.EntityFind#selectField(String) */
    EntityFind selectField(String fieldToSelect) {
        if (!this.fieldsToSelect) this.fieldsToSelect = new ListOrderedSet()
        if (fieldToSelect) this.fieldsToSelect.add(fieldToSelect)
        return this
    }

    /** @see org.moqui.entity.EntityFind#selectFields(Collection<String>) */
    EntityFind selectFields(Collection<String> fieldsToSelect) {
        if (!this.fieldsToSelect) this.fieldsToSelect = new ListOrderedSet()
        if (fieldsToSelect) this.fieldsToSelect.addAll(fieldsToSelect)
        return this
    }

    /** @see org.moqui.entity.EntityFind#getSelectFields() */
    List<String> getSelectFields() { return this.fieldsToSelect ? this.fieldsToSelect.asList() : null }

    /** @see org.moqui.entity.EntityFind#orderBy(String) */
    EntityFind orderBy(String orderByFieldName) {
        if (!this.orderByFields) this.orderByFields = new ArrayList()
        if (orderByFieldName) {
            if (orderByFieldName.contains(",")) {
                for (String obsPart in orderByFieldName.split(",")) this.orderByFields.add(obsPart.trim())
            } else {
                this.orderByFields.add(orderByFieldName)
            }
        }
        return this
    }

    /** @see org.moqui.entity.EntityFind#orderBy(List<String>) */
    EntityFind orderBy(List<String> orderByFieldNames) {
        if (!this.orderByFields) this.orderByFields = new ArrayList()
        if (orderByFieldNames) this.orderByFields.addAll(orderByFieldNames)
        return this
    }

    /** @see org.moqui.entity.EntityFind#getOrderBy() */
    List<String> getOrderBy() { return this.orderByFields ? Collections.unmodifiableList(this.orderByFields) : null }

    /** @see org.moqui.entity.EntityFind#useCache(boolean) */
    EntityFind useCache(Boolean useCache) { this.useCache = useCache; return this }

    /** @see org.moqui.entity.EntityFind#getUseCache() */
    boolean getUseCache() { return this.useCache }

    // ======================== Advanced Options ==============================

    /** @see org.moqui.entity.EntityFind#distinct(boolean) */
    EntityFind distinct(boolean distinct) { this.distinct = distinct; return this }
    /** @see org.moqui.entity.EntityFind#getDistinct() */
    boolean getDistinct() { return this.distinct }

    /** @see org.moqui.entity.EntityFind#offset(int) */
    EntityFind offset(Integer offset) { this.offset = offset; return this }
    /** @see org.moqui.entity.EntityFind#offset(int, int) */
    EntityFind offset(int pageIndex, int pageSize) { offset(pageIndex * pageSize) }
    /** @see org.moqui.entity.EntityFind#getOffset() */
    Integer getOffset() { return this.offset }

    /** @see org.moqui.entity.EntityFind#limit(int) */
    EntityFind limit(Integer limit) { this.limit = limit; return this }
    /** @see org.moqui.entity.EntityFind#getLimit() */
    Integer getLimit() { return this.limit }

    /** @see org.moqui.entity.EntityFind#forUpdate(boolean) */
    EntityFind forUpdate(boolean forUpdate) { this.forUpdate = forUpdate; return this }
    /** @see org.moqui.entity.EntityFind#getForUpdate() */
    boolean getForUpdate() { return this.forUpdate }


    // ======================== Misc Methods ========================
    protected EntityDefinition getEntityDef() {
        if (this.entityDef) return this.entityDef
        if (this.dynamicView) {
            this.entityDef = this.dynamicView.makeEntityDefinition()
        } else {
            this.entityDef = this.efi.getEntityDefinition(this.entityName)
        }
        return this.entityDef
    }

    protected boolean shouldCache() {
        if (this.dynamicView) return false
        String entityCache = this.getEntityDef().getEntityNode()."@use-cache"
        return ((this.useCache == Boolean.TRUE && entityCache != "never") || entityCache == "true")
    }

    @Override
    String toString() {
        return "Find: ${entityName} WHERE [${simpleAndMap}] [${whereEntityCondition}] HAVING [${havingEntityCondition}] " +
                "SELECT [${fieldsToSelect}] ORDER BY [${orderByFields}] CACHE [${useCache}] DISTINCT [${distinct}] " +
                "OFFSET [${offset}] LIMIT [${limit}] FOR UPDATE [${forUpdate}]"
    }


    // stubbed out abstract methods
    EntityDynamicView makeEntityDynamicView() { return null }
    EntityFind resultSetConcurrency(int resultSetConcurrency) { return null }
    int getResultSetConcurrency() { return null }
    EntityFind fetchSize(Integer fetchSize) { return null }
    Integer getFetchSize() { return null }
    EntityValue one() throws EntityException { return null }
    EntityList list() throws EntityException { return null }
    EntityListIterator iterator() throws EntityException { return null }
    long count() throws EntityException { return null }
    long updateAll(Map<String, ?> fieldsToSet) throws EntityException { return null }
    long deleteAll() throws EntityException { return null }
    EntityFind maxRows(Integer maxRows) { return null }
    Integer getMaxRows() { return null }
    EntityFind resultSetType(int resultSetType) { return null }
    int getResultSetType() { return null }
}
