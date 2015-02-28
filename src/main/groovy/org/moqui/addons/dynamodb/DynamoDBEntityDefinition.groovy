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
import org.moqui.entity.EntityFacade
import org.moqui.impl.entity.EntityFacadeImpl
import org.moqui.entity.*


class DynamoDBEntityDefinition extends EntityDefinition {

    DynamoDBEntityDefinition(EntityFacadeImpl efi, Node entityNode) {
        super(efi, entityNode)
    }

    String getRangeFieldName() {
        List<Node> fieldNodes = ed.getFieldNodes(false, true, false)
        String fieldName
        for (Node nd in fieldNodes) {
                if (nd."@is-range" == "true") {
                    fieldName = nd."@name"
                }
        }
        return fieldName
    }
}
