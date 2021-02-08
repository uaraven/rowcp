package net.ninjacat.rowcp.data

import java.sql.PreparedStatement
import java.sql.Types


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
    val outbound: Set<Relationship>
)


data class ColumnData(val columnName: String, val type: Int, val value: Any?) {
    fun isString(): Boolean {
        return stringTypes.contains(type)
    }

    fun isNull(): Boolean = value == null

    fun valueAsString(): String {
        return when {
            isNull() -> "NULL"
            isString() -> "'${escapeSql(value.toString())}'"
            else -> value.toString()
        }
    }

    private fun escapeSql(sqlString: String): String = sqlString.replace("'", "''")

    fun condition(alias: String): String {
        return when {
            isNull() -> "$alias.$columnName IS NULL"
            else -> "$alias.$columnName = ${valueAsString()}"
        }
    }

    fun parametrizedCondition(alias: String): String {
        return when {
            isNull() -> "$alias.$columnName IS NULL"
            else -> "$alias.$columnName = ?"
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
            Types.LONGVARCHAR, Types.LONGNVARCHAR,
            Types.DATE, Types.TIME, Types.TIMESTAMP, Types.TIME_WITH_TIMEZONE, Types.TIMESTAMP_WITH_TIMEZONE
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

    fun asParametrizedFilter(alias: String = ""): String {
        val tableRef = if (alias != "") alias else tableName
        return columns.joinToString(" AND ", "(", ")") {
            it.parametrizedCondition(tableRef)
        }
    }

    fun setAllParameters(statement: PreparedStatement) {
        columns.filter { !it.isNull() }.forEachIndexed { index, column ->
            column.addParameter(index + 1, statement)
        }
    }

    fun addParameters(statement: PreparedStatement) {
        columns.forEachIndexed { index, column ->
            column.addParameter(index + 1, statement)
        }
    }

    fun isNotEmpty(): Boolean = columns.isNotEmpty()

    fun dataOnly(): List<Any?> = columns.map { it.value }
}

