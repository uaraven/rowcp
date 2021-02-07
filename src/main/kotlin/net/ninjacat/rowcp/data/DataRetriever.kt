package net.ninjacat.rowcp.data

import net.ninjacat.rowcp.*
import net.ninjacat.rowcp.query.Query
import java.sql.*

data class ColumnData(val columnName: String, val type: Int, val value: Any?) {
    fun isString(): Boolean {
        return stringTypes.contains(type)
    }

    fun isNull(): Boolean = value == null

    fun valueAsString(): String {
        return when {
            isNull() -> "NULL"
            isString() -> "'${value.toString()}'"
            else -> value.toString()
        }
    }

    fun condition(alias: String): String {
        return when {
            isNull() -> "$alias.$columnName IS NULL"
            else -> "$alias.$columnName = ${valueAsString()}"
        }
    }

    fun addParameter(index: Int, statement: PreparedStatement) {
        when {
            isNull() -> statement.setNull(index, type)
            else -> statement.setObject(index, value, type)
        }
    }

    companion object {
        val stringTypes = setOf(
            Types.CHAR, Types.NCHAR, Types.VARCHAR, Types.NVARCHAR,
            Types.LONGVARCHAR, Types.LONGNVARCHAR
        )
    }
}


data class DataRow(val tableName: String, val columns: List<ColumnData>) {
    fun asFilter(alias: String = ""): String {
        val tableRef = if (alias != "") alias else tableName
        return columns.joinToString(" AND ", "(", ")") {
            it.condition(tableRef)
        }
    }

    fun addParameters(statement: PreparedStatement) {
        columns.forEachIndexed { index, column ->
            column.addParameter(index, statement)
        }
    }

    fun isNotEmpty(): Boolean = columns.isNotEmpty()
}


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

data class SelectQuery(val sources: String, val filters: List<String>) {
    fun queryList(): List<String> = if (filters.isEmpty()) listOf(sources) else filters.map { "$sources $it" }
}

class DataRetriever(params: Args, private val schema: DbSchema) {

    lateinit var sourceConnection: Connection
    lateinit var schemaGraph: SchemaGraph
    lateinit var processedRelationships: MutableSet<Relationship>
    lateinit var preparedRows: MutableSet<DataRow>
    val chunkSize = params.chunkSize

    fun collectDataToCopy(query: Query, schemaGraph: SchemaGraph): List<DataRow> {
        sourceConnection = schema.connection
        this.schemaGraph = schemaGraph

        log(V_VERBOSE, "Starting data retrieval from @|yellow ${query.table}|@")
        val startingNode = schemaGraph.tables[query.table]!!
        val select = SelectQuery(
            "SELECT ${if (query.selectDistinct) "DISTINCT" else ""} * FROM ${query.table}",
            if (query.filter != "") listOf("\nWHERE\n${query.filter}") else listOf()
        )

        processedRelationships = mutableSetOf()
        preparedRows = mutableSetOf()

        return walk(startingNode, select)
    }

    fun walk(node: Table, selectQuery: SelectQuery): List<DataRow> {
        log(V_NORMAL, "Reading table @|yellow ${node.name}|@")
        val rows = retrieveRows(node.name, selectQuery)
        log(V_VERBOSE, "Retrieved @|yellow ${rows.size}|@ rows")
        val beforeRows: List<DataRow> = if (rows.isNotEmpty()) {
            node.inbound.flatMap {
                val parentNode = schemaGraph.tables[it.sourceTable]!!
                return@flatMap if (!processedRelationships.contains(it)) { // skip this relationship if we've seen it
                    log(V_VERBOSE, "Processing relationship @|cyan ${parentNode.name}|@ -> @|blue ${node.name}|@")
                    processedRelationships.add(it)
                    val query = buildParentQuery(it, rows)
                    walk(parentNode, query)
                } else {
                    listOf()
                }
            }
        } else listOf()
        val afterRows: List<DataRow> = if (rows.isNotEmpty()) {
            node.outbound.flatMap {
                val childNode = schemaGraph.tables[it.targetTable]!!
                return@flatMap if (!processedRelationships.contains(it)) { // skip this relationship if we've seen it
                    log(V_VERBOSE, "Processing relationship @|blue ${node.name}|@ -> @|cyan ${childNode.name}|@")
                    processedRelationships.add(it)
                    val query = buildChildQuery(it, rows)
                    walk(childNode, query)
                } else {
                    listOf()
                }
            }
        } else listOf()
        return beforeRows + rows + afterRows
    }

    // TODO: Make parametrized, maybe
    private fun buildParentQuery(relationship: Relationship, rows: List<DataRow>): SelectQuery {
        val baseQuery =
            with(StringBuilder("SELECT parent.* FROM ${relationship.sourceTable} parent JOIN ${relationship.targetTable} child ON\n")) {
                append(
                    relationship.columnMap.joinToString(" AND ", "(", ")") {
                        "parent.${it.sourceColumn} = child.${it.targetColumn}"
                    }
                )
            }.toString()

        val filters =
            rows.chunked(chunkSize).map { row -> "\nWHERE\n" + row.joinToString("\n OR ") { it.asFilter("child") } }
        return SelectQuery(baseQuery, filters)
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

        val filters =
            rows.chunked(chunkSize).map { row -> "\nWHERE\n" + row.joinToString("\n OR ") { it.asFilter("parent") } }
        return SelectQuery(baseQuery, filters)
    }

    private fun retrieveRows(tableName: String, queries: SelectQuery): List<DataRow> {
        log(V_SQL, "Executing query:\n${queries.sources}")
        return queries.queryList().flatMap {
            try {
                val statement = sourceConnection.createStatement()
                val rs = statement.executeQuery(it)
                val result = resultSetToDataRows(rs, tableName)
                statement.close()
                result
            } catch (s: SQLSyntaxErrorException) {
                log(V_NORMAL, "Query failed @|red ${s.message}|@\n---\n$it\n---\n")
                throw s
            }
        }
    }

    private fun resultSetToDataRows(rs: ResultSet, tableName: String): List<DataRow> {
        val result = mutableListOf<DataRow>()
        while (rs.next()) {
            val row = toDataRow(rs, tableName)
            if (row.isNotEmpty()) {
                result.add(row)
            }
        }
        rs.close()
        return result.toList()
    }

    private fun toDataRow(rs: ResultSet, tableName: String): DataRow {
        val result = mutableListOf<ColumnData>()
        for (i in 1 until rs.metaData.columnCount) {
            val columnName = rs.metaData.getColumnName(i)
            val columnType = rs.metaData.getColumnType(i)
            val value = rs.getObject(i)
            result.add(ColumnData(columnName, columnType, value))
        }
        return DataRow(tableName, result.toList())
    }
}