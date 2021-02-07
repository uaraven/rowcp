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

data class RelationshipDirection(val sourceTable: String, val targetTable: String)

data class Relationship(val sourceTable: String, val targetTable: String, val columnMap: List<ColumnMap>)

data class TableNode(
    val name: String,
    val columns: List<Column>,
    val inbound: Set<Relationship>,
    val outbound: Set<Relationship>
)

data class SchemaGraph(
    val tables: Map<String, TableNode>,
    val relationships: Map<RelationshipDirection, Relationship>
)

data class Table(
    val name: String,
    val columns: List<Column>,
    val parents: List<Relationship>,
    val children: List<Relationship>
) {
    fun getChildrenForTable(tableName: String): List<Relationship> = children.filter { it.targetTable == tableName }
    fun getParentsForTable(tableName: String): List<Relationship> = parents.filter { it.targetTable == tableName }
}

class DbSchema(args: Args) {

    val connection =
        DriverManager.getConnection(args.sourceJdbcUrl, args.nullableSourceUser(), args.nullableSourcePassword())!!

    fun buildSchemaGraph(): SchemaGraph {
        log(V_NORMAL, "Building source schema graph")
        val resultSet = connection.metaData.getTables(
            null, null, null, null
        )
        val tables: MutableMap<String, Table> = mutableMapOf()
        while (resultSet.next()) {
            val tableName = resultSet.getString("TABLE_NAME")
            val parents = getParents(tableName)
            val children = getChildren(tableName)
            val columns = getTableColumns(tableName)
            val table = Table(tableName, columns, parents, children)
            tables[table.name] = table
        }
        log(V_VERBOSE, "Collected metadata of @|yellow ${tables.size}|@ tables")

        val relationships = mutableMapOf<RelationshipDirection, Relationship>()
        val nodes = mutableMapOf<String, TableNode>()

        tables.values.map {
            val inboundRelationships = (it.parents/* + findParents(tables, it.name)*/).toSet()
            val outboundRelationships = (it.children /*+ findChildren(tables, it.name)*/).toSet()
            nodes[it.name] = TableNode(
                it.name,
                it.columns,
                inboundRelationships,
                outboundRelationships
            )
            (inboundRelationships + outboundRelationships)
                .forEach { rel -> relationships[RelationshipDirection(rel.sourceTable, rel.targetTable)] = rel }

        }

        return SchemaGraph(nodes.toMap(), relationships.toMap())
    }

    private fun getChildren(tableName: String): List<Relationship> {
        val resultSet = connection.metaData.getExportedKeys(null, null, tableName)
        val results = mutableListOf<Relationship>()
        var target = ""
        val thisColumns = mutableListOf<String>()
        val targetColumns = mutableListOf<String>()
        while (resultSet.next()) {
            val targetTableName = resultSet.getString("FKTABLE_NAME")
            val targetColumn = resultSet.getString("FKCOLUMN_NAME")
            val sourceColumn = resultSet.getString("PKCOLUMN_NAME")
            if (target != targetTableName) {
                if (target != "") {
                    results.add(
                        Relationship(tableName, target, zipColumns(thisColumns, targetColumns))
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
            results.add(Relationship(tableName, target, zipColumns(thisColumns, targetColumns)))
        }
        return results.toList()
    }

    private fun getParents(tableName: String): List<Relationship> {
        val resultSet = connection.metaData.getImportedKeys(null, null, tableName)
        val results = mutableListOf<Relationship>()
        var source = ""
        val sourceColumns = mutableListOf<String>()
        val targetColumns = mutableListOf<String>()
        while (resultSet.next()) {
            val sourceTableName = resultSet.getString("PKTABLE_NAME")
            val sourceColumn = resultSet.getString("PKCOLUMN_NAME")
            val targetColumn = resultSet.getString("FKCOLUMN_NAME")
            if (source != sourceTableName) {
                if (source != "") {
                    results.add(
                        Relationship(source, tableName, zipColumns(sourceColumns, targetColumns))
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
            results.add(Relationship(source, tableName, zipColumns(sourceColumns, targetColumns)))
        }
        return results.toList()
    }

    private fun getTableColumns(tableName: String): List<Column> {
        val resultSet = connection.metaData.getColumns(null, null, tableName, null)
        val columns = mutableListOf<Column>()
        while (resultSet.next()) {
            val name = resultSet.getString("COLUMN_NAME")
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