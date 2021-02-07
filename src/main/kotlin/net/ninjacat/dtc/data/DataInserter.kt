package net.ninjacat.dtc.data

import net.ninjacat.dtc.Args
import net.ninjacat.dtc.V_NORMAL
import net.ninjacat.dtc.log
import java.io.Closeable
import java.sql.DriverManager

data class InsertBatch(val statement: String, val data: List<DataRow>)

class DataInserter(private val args: Args) {

    private val conn =
        DriverManager.getConnection(args.targetJdbcUrl, args.nullableTargetUser(), args.nullableTargetPassword())

    fun prepareBatches(rows: List<DataRow>, schemaGraph: SchemaGraph): List<InsertBatch> {
        val groupedByTable = rows.groupBy { row -> row.tableName }

        return groupedByTable.flatMap { entry ->
            val (name, columns, _, _) = schemaGraph.tables[entry.key]!!
            val baseInsert = "INSERT INTO $name(${columns.joinToString(",") { it.name }})\n" +
                    "VALUES(${columns.joinToString(",") { "?" }})"

            entry.value.chunked(args.chunkSize).map { InsertBatch(baseInsert, it) }
        }
    }

    fun runBatches(batches: List<InsertBatch>) {
        conn.autoCommit = false

        var lastPcnt = 0
        try {
            batches.forEachIndexed { index, batch ->
                runBatch(batch)
                val pcnt = index * 100 / batches.size
                if (pcnt - lastPcnt > 10) {
                    log(V_NORMAL, "Inserted @|blue ${pcnt}$$")
                    lastPcnt = pcnt
                }
            }
            conn.commit()
        } catch (e: Exception) {
            conn.rollback()
            throw e
        }
    }

    fun runBatch(batch: InsertBatch) {
        val statement = conn.prepareStatement(batch.statement)
        (statement as Closeable).use {
            batch.data.forEach { row ->
                row.addParameters(statement)
                statement.addBatch()
            }
            if (!args.dryRun) {
                statement.executeBatch()
            }
        }
    }
}

