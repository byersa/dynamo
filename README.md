dynamo
======

DynamoDB component

To use this component, you must have this code in your MoquiDevConf.xml file:

    <entity-facade crypt-pass="MoquiDevDefaultPassword:CHANGEME">
    ...
        <datasource group-name="transactional_nosql" object-factory="org.moqui.impl.entity.dynamodb.DynamoDBDatasourceFactory">
            <inline-other dynamodb-accessKey="AKIAIBRXVHUM37OTC4MQ" dynamodb-secretAccessKey="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"/>
        </datasource>

Add the group-name attribute to entity elements as needed to point them to the new datasource; for example:
     <entity entity-name="Person" package-name="ccoach" group-name="transactional_nosql">
     
The group-name is the key to understanding how Moqui distinguishes between databases. In EntityFacadeImpl, this is the code for "makeValue":     
    EntityValue makeValue(String entityName) {
        EntityDatasourceFactory edf = datasourceFactoryByGroupMap.get(getEntityGroupName(entityName))
        return edf.makeEntityValue(entityName)
    }
    
It uses the datasource factory to make the value.

True "query" capability is not, currently, a part of the DynamoDB component. In DynamoDB, a field query forces a scan of the 
whole table, and since DynamoDB tables are often quite large, this functionality would not be used often. But DynamoDB uses 
a two key primary key approach that can result in multiple records being returned. The first key, the "hash" key, can act 
like a primary key and be unique. It can also act in conjunction with the second primary key, the "range" key - with the 
combination being unique. The range key can be queried with the usual conditional operators, thus allowing multiple values 
to be returned.

See the test groovy file for examples of how it is used.