package net.ninjacat.rowcp.data

import net.ninjacat.rowcp.V_NORMAL
import net.ninjacat.rowcp.V_VERBOSE
import net.ninjacat.rowcp.data.Utils.use
import net.ninjacat.rowcp.log
import java.sql.DatabaseMetaData.bestRowTemporary
import java.sql.DriverManager
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

class DbSchema(val name: String, val jdbcUrl: String, user: String?, password: String?) {

    val connection = DriverManager.getConnection(jdbcUrl, user, password)!!

    private val schema = lazy { buildSchemaGraph() }

    fun getSchemaGraph(): SchemaGraph = schema.value

    private fun Double.format(digits: Int) = "%.${digits}f".format(this)

    private fun buildSchemaGraph(): SchemaGraph {
        log(V_NORMAL, "Building $name schema graph")
        val start = Instant.now()
        val resultSet = connection.metaData.getTables(
            null, null, null, arrayOf("TABLE")
        )
        val tableNames = mutableSetOf<String>()
        while (resultSet.next()) {
            tableNames.add(resultSet.getString("TABLE_NAME"))
        }
        log(V_NORMAL, "Tables to scan: ${tableNames.size}")
        val counter = AtomicInteger(0)
        val tables = ConcurrentHashMap<String, Table>()
        tableNames.parallelStream().forEach { tableName ->
            tables.computeIfAbsent(tableName.lowercase()) { tableN ->
                val parents = getParents(tableName)
                val children = getChildren(tableName)
                val columns = getTableColumns(tableName)
                val pk = getPrimaryKey(tableName)
                val table = Table(tableN, columns, parents, children, pk)
                val percentComplete = counter.incrementAndGet() * 100.0 / tableNames.size
                log(V_VERBOSE, "\r${percentComplete.format(1)}%    ", noLineFeed = true)
                table
            }
        }
        val done = Duration.between(start, Instant.now())
        log(V_VERBOSE, "\rCollected metadata in @|cyan ${done.toMinutes()}:${"%02d".format(done.toSecondsPart())} |@")

        buildReverseParents(tables)

        return SchemaGraph(tables.toMap())
    }

    /**
     * Some databases (looking at you MySql) do not return anything in getImportedKeys(). To properly handle
     * incoming relationships we need to scan all the tables and check if any of the tables act as a parent
     * for others
     */
    private fun buildReverseParents(tables: MutableMap<String, Table>) {
        tables.forEach { (name, table) ->
            table.outbound.forEach { relation ->
                val child = tables[relation.targetTable]
                val parent = child!!.findParent(name)
                if (parent == null) { // we're missing a relationship here!
                    child.inbound.add(Relationship(relation.sourceTable, relation.targetTable, relation.columnMap))
                }
            }
        }
    }

    private fun getPrimaryKey(tableName: String?): Set<String> {
        return connection.metaData.getPrimaryKeys(null, null, tableName).use {
            val results = mutableListOf<String>()
            while (next()) {
                results.add(getString("COLUMN_NAME").lowercase())
            }
            if (results.isEmpty()) {
                getUniqueKey(tableName)
            } else {
                results.toSet()
            }
        }
    }

    private fun getUniqueKey(tableName: String?): Set<String> {
        return connection.metaData.getBestRowIdentifier(null, null, tableName!!, bestRowTemporary, false).use {
            val results = mutableListOf<String>()
            if (next()) {
                results.add(getString("COLUMN_NAME").lowercase())
            }
            results.toSet()
        }
    }

    private fun getChildren(tableName: String): Set<Relationship> {
        val resultSet = connection.metaData.getExportedKeys(null, null, tableName)
        val results = mutableListOf<Relationship>()
        var target = ""
        val thisColumns = mutableListOf<String>()
        val targetColumns = mutableListOf<String>()
        try {
            while (resultSet.next()) {
                val targetTableName = resultSet.getString("FKTABLE_NAME").lowercase()
                val targetColumn = resultSet.getString("FKCOLUMN_NAME").lowercase()
                val sourceColumn = resultSet.getString("PKCOLUMN_NAME").lowercase()
                if (target != targetTableName) {
                    if (target != "") {
                        results.add(
                            Relationship(tableName.lowercase(), target, zipColumns(thisColumns, targetColumns))
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
                results.add(Relationship(tableName.lowercase(), target, zipColumns(thisColumns, targetColumns)))
            }
            return results.toSet()
        } finally {
            resultSet.close()
        }
    }

    private fun getParents(tableName: String): MutableSet<Relationship> {
        val resultSet = connection.metaData.getImportedKeys(null, null, tableName)
        val results = mutableListOf<Relationship>()
        var source = ""
        val sourceColumns = mutableListOf<String>()
        val targetColumns = mutableListOf<String>()
        try {
            while (resultSet.next()) {
                val sourceTableName = resultSet.getString("PKTABLE_NAME").lowercase()
                val sourceColumn = resultSet.getString("PKCOLUMN_NAME").lowercase()
                val targetColumn = resultSet.getString("FKCOLUMN_NAME").lowercase()
                if (source != sourceTableName) {
                    if (source != "") {
                        results.add(
                            Relationship(source, tableName.lowercase(), zipColumns(sourceColumns, targetColumns))
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
                results.add(Relationship(source, tableName.lowercase(), zipColumns(sourceColumns, targetColumns)))
            }
            return results.toMutableSet()
        } finally {
            resultSet.close()
        }
    }

    private fun getTableColumns(tableName: String): List<Column> {
        val resultSet = connection.metaData.getColumns(null, null, tableName, null)
        val columns = mutableListOf<Column>()
        try {
            while (resultSet.next()) {
                val name = resultSet.getString("COLUMN_NAME").lowercase()
                val type = resultSet.getInt("DATA_TYPE")
                columns.add(Column(name, type))
            }
            return columns.toList()
        } finally {
            resultSet.close()
        }
    }


    private fun zipColumns(
        sourceColumns: MutableList<String>,
        targetColumns: MutableList<String>
    ) = sourceColumns.zip(targetColumns).map { ColumnMap.from(it.first, it.second) }

}