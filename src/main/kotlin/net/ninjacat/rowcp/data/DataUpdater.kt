package net.ninjacat.rowcp.data

import net.ninjacat.rowcp.*
import net.ninjacat.rowcp.data.Utils.use
import java.sql.PreparedStatement

data class RowUpdate(val statement: String, val data: DataRow, val update: Boolean) {
    fun setStatementParameters(statement: PreparedStatement) {
        if (update) {
            data.addParametersForUpdate(statement)
        } else {
            data.addParametersForInsert(statement)
        }
    }
}

class DataUpdater(private val args: Args, schema: DbSchema) : DataWriter {

    private val conn = schema.connection
    private val schemaGraph = schema.getSchemaGraph()
    private val writtenRows = mutableSetOf<DataRow>()

    private fun doesAlreadyExist(row: DataRow): Boolean {
        val checkStatement =
            conn.prepareStatement("select count(*) from ${row.tableName()} t WHERE ${row.asParametrizedFilter("t")}")
        return checkStatement.use {
            row.addParametersForSelect(this)
            executeQuery().use {
                (next() && getInt(1) > 0)
            }
        }
    }

    fun prepareUpdates(startingNode: DataNode): List<RowUpdate> {
        val beforeUpdates = startingNode.before.flatMap { prepareUpdates(it) }
        val afterUpdates = startingNode.after.flatMap { prepareUpdates(it) }

        log(V_VERBOSE, "Preprocessing rows for ${startingNode.tableName}")
        if (schemaGraph.table(startingNode.tableName) == null) {
            throw RuntimeException("No table '${startingNode.tableName}' in the target database")
        }
        val (name, columns, _, _) = schemaGraph.tables[startingNode.tableName]!!

        val updates = startingNode.rows.map { row ->
            if (doesAlreadyExist(row)) {
                RowUpdate("UPDATE $name SET\n" +
                        row.nonKeyColumns().joinToString(", ") { "${it.columnName} = ?" } + "\nWHERE\n" +
                        row.asParametrizedFilter(""), row, true)
            } else {
                RowUpdate(
                    "INSERT INTO $name(${columns.joinToString(",") { it.name }})\n" +
                            "VALUES(${columns.joinToString(",") { "?" }})", row, false
                )
            }
        }

        return beforeUpdates + updates + afterUpdates
    }

    fun runUpdates(updates: List<RowUpdate>) {
        conn.autoCommit = false

        var lastPcnt = 0
        try {
            updates.forEachIndexed { index, update ->
                runUpdate(update)
                val pcnt = index * 100 / updates.size
                if (pcnt - lastPcnt > 10) {
                    log(V_NORMAL, "Updated @|blue ${pcnt}|@%")
                    lastPcnt = pcnt
                }
            }
            log(V_NORMAL, "Updated @|blue 100|@%")
            conn.commit()
        } catch (e: Exception) {
            conn.rollback()
            throw e
        }
        conn.autoCommit = true
    }

    private fun runUpdate(update: RowUpdate) {
        log(V_SQL, "Executing statement:\n${update.statement}")
        conn.prepareStatement(update.statement).use {
            if (!writtenRows.contains(update.data)) {
                update.setStatementParameters(this)
                writtenRows.add(update.data)
            }
            if (!args.dryRun) {
                val count = executeUpdate()
                log(V_SQL, "Updated $count records")
            }
        }
    }

    override fun writeData(startingNode: DataNode) {
        val updates = prepareUpdates(startingNode)
        log(V_VERBOSE, "Preparing to run @|yellow ${updates.size}|@ INSERT/UPDATE statements")
        runUpdates(updates)
    }
}

