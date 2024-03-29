package net.ninjacat.rowcp.data

import net.ninjacat.rowcp.*
import net.ninjacat.rowcp.query.Query
import java.lang.Integer.min
import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLSyntaxErrorException
import kotlin.system.exitProcess

data class SelectRelationship(
    val sourceTableName: String,
    val targetTableName: String,
    val joinRelationship: Relationship,
    val sourceTable: TableRowsNode
)

data class TableRowsNode(
    val tableName: String, val dataRows: List<DataRow>,
    val parents: List<SelectRelationship>,
    val children: List<SelectRelationship>
)

data class SelectQuery(val select: String, val filters: List<String>, val parameters: List<List<ColumnData>>) {
    fun queryList(): List<Pair<String, List<ColumnData>>> = when {
        filters.isEmpty() -> {
            listOf(Pair(select, listOf()))
        }
        parameters.isEmpty() -> {
            filters.map { Pair("$select $it", listOf()) }
        }
        else -> {
            filters.zip(parameters).map { (filter, param) -> Pair("$select $filter", param) }
        }
    }
}

data class DataNode(
    val tableName: String,
    val rows: List<DataRow>,
    val before: List<DataNode>,
    val after: List<DataNode>
) {
    fun size(): Int = rows.size + before.map { it.size() }.sum() + after.map { it.size() }.sum()
}

class DataRetriever(params: Args, private val schema: DbSchema) {

    lateinit var sourceConnection: Connection
    lateinit var processedRelationships: MutableSet<Relationship>
    lateinit var preparedRows: MutableSet<DataRow>
    private val schemaGraph: SchemaGraph = schema.getSchemaGraph()
    private val chunkSize = params.chunkSize
    private val tableFilter = TableFilter(params.tablesToSkip)

    fun collectDataToCopy(query: Query): DataNode {
        sourceConnection = schema.connection

        log(V_VERBOSE, "Starting data retrieval from @|yellow ${query.table}|@")
        if (!schemaGraph.tables.containsKey(query.table)) {
            logError("Table ${query.table} not found in source schema")
            exitProcess(-1)
        }
        val startingNode = schemaGraph.tables[query.table]!!
        val select = SelectQuery(
            "SELECT ${if (query.selectDistinct) "DISTINCT" else ""} * FROM ${query.tableName()}",
            (if (query.filter != "") listOf("\nWHERE\n${query.filter}") else listOf()), listOf()
        )

        processedRelationships = mutableSetOf()
        preparedRows = mutableSetOf()

        return walk(startingNode, select, WalkDirection.BOTH)
    }

    fun walk(node: Table, selectQuery: SelectQuery, walkDirection: WalkDirection): DataNode {
        log(V_NORMAL, "Reading table @|yellow ${node.name}|@")
        val rows = retrieveRows(node, selectQuery)
        log(V_VERBOSE, "Retrieved @|yellow ${rows.size}|@ rows")
        val before: List<DataNode> =
            if (rows.isNotEmpty() && (walkDirection == WalkDirection.BOTH || walkDirection == WalkDirection.PARENTS)) {
                node.inbound.flatMap {
                    val parentNode = schemaGraph.tables[it.sourceTable]!!
                    return@flatMap if (!processedRelationships.contains(it)) { // skip this relationship if we've seen it
                        log(V_VERBOSE, "Processing relationship @|cyan ${node.name}|@ <- @|blue ${parentNode.name}|@")
                        processedRelationships.add(it)
                        if (tableFilter.shouldSkip(it.sourceTable)) {
                            log(V_NORMAL, "Skipping table @|yellow ${it.sourceTable}|@")
                            listOf()
                        } else {
                            val query = buildParentQuery(it, rows)
                            listOf(walk(parentNode, query, WalkDirection.PARENTS))
                        }
                    } else {
                        listOf()
                    }
                }
            } else listOf()
        val after: List<DataNode> =
            if (rows.isNotEmpty() && (walkDirection == WalkDirection.CHILDREN || walkDirection == WalkDirection.BOTH)) {
                node.outbound.flatMap {
                    val childNode = schemaGraph.tables[it.targetTable]!!
                    return@flatMap if (!processedRelationships.contains(it)) { // skip this relationship if we've seen it
                        log(V_VERBOSE, "Processing relationship @|blue ${node.name}|@ -> @|cyan ${childNode.name}|@")
                        processedRelationships.add(it)
                        if (tableFilter.shouldSkip(it.targetTable)) {
                            log(V_NORMAL, "Skipping table @|yellow ${it.targetTable}|@")
                            listOf()
                        } else {
                            val query = buildChildQuery(it, rows)
                            listOf(walk(childNode, query, walkDirection))
                        }
                } else {
                    listOf()
                }
            }
        } else listOf()
        return DataNode(node.name, rows, before, after)
    }

    private fun buildParentQuery(relationship: Relationship, rows: List<DataRow>): SelectQuery {
        val baseQuery =
            with(StringBuilder("SELECT parent.* FROM ${relationship.sourceTable} parent JOIN ${relationship.targetTable} child ON\n")) {
                append(
                    relationship.columnMap.joinToString(" AND ", "(", ")") {
                        "parent.${it.sourceColumn} = child.${it.targetColumn}"
                    }
                )
            }.toString()

        val adjustedChunkSize = calculateChunkSize(rows)
        val filters =
            rows.chunked(adjustedChunkSize)
                .map { row -> "\nWHERE\n" + row.joinToString("\n OR ") { it.asParametrizedFilter("child") } }
        val parameters = rows.chunked(adjustedChunkSize).map { rowChunk -> rowChunk.flatMap { it.primaryKey() } }
        return SelectQuery(baseQuery, filters, parameters)
    }

    private fun buildChildQuery(relationship: Relationship, rows: List<DataRow>): SelectQuery {
        val baseQuery =
            with(StringBuilder("SELECT child.* FROM ${relationship.targetTable} child JOIN ${relationship.sourceTable} parent ON\n")) {
                append(
                    relationship.columnMap.joinToString(" AND ", "(", ")") {
                        "parent.${it.sourceColumn} = child.${it.targetColumn}"
                    }
                )
            }.toString()

        val adjustedChunkSize = calculateChunkSize(rows)

        val filters =
            rows.chunked(adjustedChunkSize)
                .map { row -> "\nWHERE\n" + row.joinToString("\n OR ") { it.asParametrizedFilter("parent") } }
        val parameters = rows.chunked(adjustedChunkSize).map { rowChunk -> rowChunk.flatMap { it.primaryKey() } }
        return SelectQuery(baseQuery, filters, parameters)
    }

    private fun calculateChunkSize(rows: List<DataRow>): Int {
        if (rows.isEmpty()) {
            return chunkSize
        }
        val paramsPerRow = rows.first().primaryKey().size
        return min(chunkSize, 900 / paramsPerRow) // allow up to 900 parameters (to support sqlite 999 parameter limit)
    }

    private fun retrieveRows(table: Table, queries: SelectQuery): List<DataRow> {
        log(V_SQL, "Executing query:\n${queries.select}\n${queries.filters}")
        return queries.queryList().flatMap {
            val statement = sourceConnection.prepareStatement(it.first)
            try {
                val rs = applyParametersAndQuery(statement, it.second)
                val result = resultSetToDataRows(rs, table)
                result
            } catch (s: SQLSyntaxErrorException) {
                log(V_NORMAL, "Query failed @|red ${s.message}|@\n---\n$it\n---\n")
                throw s
            } finally {
                statement.close()
            }
        }.toSet().toList() // clean duplicates
    }

    private fun applyParametersAndQuery(statement: PreparedStatement, second: List<ColumnData>): ResultSet {
        second.forEachIndexed { index, column ->
            statement.setObject(index + 1, column.value, column.type)
        }
        return statement.executeQuery()
    }

    private fun resultSetToDataRows(rs: ResultSet, table: Table): List<DataRow> {
        val result = mutableListOf<DataRow>()
        while (rs.next()) {
            val row = toDataRow(rs, table)
            if (row.isNotEmpty()) {
                result.add(row)
            }
        }
        rs.close()
        return result.toList()
    }

    private fun toDataRow(rs: ResultSet, table: Table): DataRow {
        val result = mutableListOf<ColumnData>()
        for (i in 1..rs.metaData.columnCount) {
            val columnName = rs.metaData.getColumnName(i).lowercase()
            val columnType = rs.metaData.getColumnType(i)
            val value = rs.getObject(i)
            result.add(ColumnData(columnName, columnType, value))
        }
        return DataRow(table, result.toList())
    }
}