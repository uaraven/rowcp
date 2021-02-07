package net.ninjacat.rowcp.data

import net.ninjacat.rowcp.Args
import net.ninjacat.rowcp.V_NORMAL
import net.ninjacat.rowcp.V_VERBOSE
import net.ninjacat.rowcp.log
import java.sql.DriverManager

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

class DbSchema(args: Args) {

    val connection =
        DriverManager.getConnection(args.sourceJdbcUrl, args.nullableSourceUser(), args.nullableSourcePassword())!!

    fun buildSchemaGraph(): SchemaGraph {
        log(V_NORMAL, "Building source schema graph")
        val resultSet = connection.metaData.getTables(
            null, null, null, arrayOf("TABLE")
        )
        val tables: MutableMap<String, Table> = mutableMapOf()
        while (resultSet.next()) {
            val tableName = resultSet.getString("TABLE_NAME")
            val parents = getParents(tableName)
            val children = getChildren(tableName)
            val columns = getTableColumns(tableName)
            val table = Table(tableName.toLowerCase(), columns, parents, children)
            tables[table.name] = table
        }
        log(V_VERBOSE, "Collected metadata of @|yellow ${tables.size}|@ tables")

        return SchemaGraph(tables.toMap())
    }

    private fun getChildren(tableName: String): Set<Relationship> {
        val resultSet = connection.metaData.getExportedKeys(null, null, tableName)
        val results = mutableListOf<Relationship>()
        var target = ""
        val thisColumns = mutableListOf<String>()
        val targetColumns = mutableListOf<String>()
        while (resultSet.next()) {
            val targetTableName = resultSet.getString("FKTABLE_NAME").toLowerCase()
            val targetColumn = resultSet.getString("FKCOLUMN_NAME").toLowerCase()
            val sourceColumn = resultSet.getString("PKCOLUMN_NAME").toLowerCase()
            if (target != targetTableName) {
                if (target != "") {
                    results.add(
                        Relationship(tableName.toLowerCase(), target, zipColumns(thisColumns, targetColumns))
                    )
                    thisColumns.clear()
                    targetColumns.clear()
                }

                target = targetTableName
            }
            thisColumns.add(sourceColumn)
            targetColumns.add(targetColumn)
        }
        if (target != "") {
            results.add(Relationship(tableName.toLowerCase(), target, zipColumns(thisColumns, targetColumns)))
        }
        return results.toSet()
    }

    private fun getParents(tableName: String): Set<Relationship> {
        val resultSet = connection.metaData.getImportedKeys(null, null, tableName)
        val results = mutableListOf<Relationship>()
        var source = ""
        val sourceColumns = mutableListOf<String>()
        val targetColumns = mutableListOf<String>()
        while (resultSet.next()) {
            val sourceTableName = resultSet.getString("PKTABLE_NAME").toLowerCase()
            val sourceColumn = resultSet.getString("PKCOLUMN_NAME").toLowerCase()
            val targetColumn = resultSet.getString("FKCOLUMN_NAME").toLowerCase()
            if (source != sourceTableName) {
                if (source != "") {
                    results.add(
                        Relationship(source, tableName.toLowerCase(), zipColumns(sourceColumns, targetColumns))
                    )
                    sourceColumns.clear()
                    targetColumns.clear()
                }

                source = sourceTableName
            }
            sourceColumns.add(sourceColumn)
            targetColumns.add(targetColumn)
        }
        if (source != "") {
            results.add(Relationship(source, tableName.toLowerCase(), zipColumns(sourceColumns, targetColumns)))
        }
        return results.toSet()
    }

    private fun getTableColumns(tableName: String): List<Column> {
        val resultSet = connection.metaData.getColumns(null, null, tableName, null)
        val columns = mutableListOf<Column>()
        while (resultSet.next()) {
            val name = resultSet.getString("COLUMN_NAME").toLowerCase()
            val type = resultSet.getInt("DATA_TYPE")
            columns.add(Column(name, type))
        }
        return columns.toList()
    }


    private fun zipColumns(
        sourceColumns: MutableList<String>,
        targetColumns: MutableList<String>
    ) = sourceColumns.zip(targetColumns).map { ColumnMap.from(it.first, it.second) }

}