/*
 *    Copyright 2021 Oleksiy Voronin
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package net.ninjacat.rowcp.data.visualizer

import net.ninjacat.rowcp.Args
import net.ninjacat.rowcp.V_NORMAL
import net.ninjacat.rowcp.data.DbSchema
import net.ninjacat.rowcp.data.Relationship
import net.ninjacat.rowcp.data.WalkDirection
import net.ninjacat.rowcp.log
import net.ninjacat.rowcp.query.QueryParser

data class TreeTable(val tableName: String, val parents: List<TreeTable>, val children: List<TreeTable>)

class CopyVisualizer(
    private val args: Args,
    private val parser: QueryParser,
    private val schema: DbSchema
) {
    lateinit var processedRelationships: MutableSet<Relationship>

    fun showCopyTree() {

        processedRelationships = mutableSetOf()
        val query = parser.parseQuery(args.getQuery())

        val root = walk(query.table, WalkDirection.BOTH)
        val text = print(root, 0)
        log(V_NORMAL, text)
    }

    fun walk(tableName: String, walkDirection: WalkDirection): TreeTable {
        val schemaGraph = schema.getSchemaGraph()
        val node = schemaGraph.table(tableName)!!
        val before: List<TreeTable> =
            if (walkDirection == WalkDirection.BOTH || walkDirection == WalkDirection.PARENTS) {
                node.inbound.flatMap {
                    val parentNode = schemaGraph.tables[it.sourceTable]!!
                    return@flatMap if (!processedRelationships.contains(it)) { // skip this relationship if we've seen it
                        processedRelationships.add(it)
                        if (args.tablesToSkip.contains(it.sourceTable)) {
                            listOf()
                        } else {
                            listOf(walk(parentNode.name, WalkDirection.PARENTS))
                        }
                    } else {
                        listOf()
                    }
                }
            } else listOf()
        val after: List<TreeTable> =
            if (walkDirection == WalkDirection.CHILDREN || walkDirection == WalkDirection.BOTH) {
                node.outbound.flatMap {
                    val childNode = schemaGraph.tables[it.targetTable]!!
                    return@flatMap if (!processedRelationships.contains(it)) { // skip this relationship if we've seen it
                        processedRelationships.add(it)
                        if (args.tablesToSkip.contains(it.targetTable)) {
                            listOf()
                        } else {
                            listOf(walk(childNode.name, walkDirection))
                        }
                    } else {
                        listOf()
                    }
                }
            } else listOf()
        return TreeTable(tableName, before, after)
    }

    private fun printPrev(table: TreeTable, offset: Int): String {
        val list = table.parents.map { print(it, offset) }
        return list.joinToString("\n")
    }

    private fun printNext(table: TreeTable, offset: Int): String {
        val list = table.children.map { print(it, offset) }
        return list.joinToString("\n")
    }

    private fun print(table: TreeTable, offset: Int): String {
        val parentLines = printPrev(table, offset + 1)
        val childLines = printNext(table, offset + 1)
        return (parentLines + "${" ".repeat(offset)}${table.tableName}\n" + childLines).replace("\n\n", "\n")
    }
}
