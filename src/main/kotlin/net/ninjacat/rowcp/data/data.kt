package net.ninjacat.rowcp.data

import java.sql.PreparedStatement

data class Column(val name: String, val type: Int)

data class ColumnMap(val sourceColumn: String, val targetColumn: String) {
    companion object {
        fun from(thisColumn: String, targetColumn: String): ColumnMap {
            return ColumnMap(thisColumn, targetColumn)
        }
    }
}

data class Relationship(val sourceTable: String, val targetTable: String, val columnMap: List<ColumnMap>)

data class SchemaGraph(
    val tables: Map<String, Table>
) {

    fun table(name: String): Table? = tables[name.toLowerCase()]
}

data class Table(
    val name: String,
    val columns: List<Column>,
    val inbound: Set<Relationship>,
    val outbound: Set<Relationship>,
    val primaryKey: Set<String>
) {
    val columnNames = columns.map { it.name }.toSet()
}

data class ColumnData(val columnName: String, val type: Int, val value: Any?) {

    private fun isNull(): Boolean = value == null

    fun parametrizedCondition(alias: String): String = when {
        isNull() -> "$alias.$columnName IS NULL"
        else -> "$alias.$columnName = ?"
    }

    fun addParameter(index: Int, statement: PreparedStatement) {
        when {
            isNull() -> statement.setNull(index, type)
            else -> statement.setObject(index, value, type)
        }
    }
}


data class DataRow(val table: Table, val columns: List<ColumnData>) {
    fun tableName(): String = table.name

    fun asParametrizedFilter(alias: String = ""): String {
        val tableRef = if (alias != "") alias else table.name
        return columns
            .filter { table.primaryKey.isEmpty() || table.primaryKey.contains(it.columnName) }
            .joinToString(" AND ", "(", ")") {
                it.parametrizedCondition(tableRef)
            }
    }

    fun addParametersForSelect(statement: PreparedStatement) {
        columns
            .filter { table.primaryKey.isEmpty() || table.primaryKey.contains(it.columnName) }
            .forEachIndexed { index, column ->
                column.addParameter(index + 1, statement)
            }
    }

    fun addParametersForInsert(statement: PreparedStatement) {
        columns
            .forEachIndexed { index, column ->
                column.addParameter(index + 1, statement)
            }
    }

    fun primaryKey(): List<ColumnData> =
        columns.filter { table.primaryKey.isEmpty() || table.primaryKey.contains(it.columnName) }
            .filter { columnData -> columnData.value != null }

    fun isNotEmpty(): Boolean = columns.isNotEmpty()

    fun dataOnly(): List<Any?> = columns.map { it.value }

    val columnNames: Set<String> by lazy { columns.map { it.columnName }.toSet() }
}

