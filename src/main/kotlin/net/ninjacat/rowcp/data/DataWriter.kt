package net.ninjacat.rowcp.data

import net.ninjacat.rowcp.*

data class InsertBatch(val statement: String, val data: List<DataRow>)

class DataWriter(private val args: Args, schema: DbSchema) {

    private val conn = schema.connection
    private val schemaGraph = schema.getSchemaGraph()
    private val writtenRows = mutableSetOf<DataRow>()

    fun prepareBatches(startingNode: DataNode): List<InsertBatch> {
        val beforeBatches = startingNode.before.flatMap { prepareBatches(it) }
        val afterBatches = startingNode.after.flatMap { prepareBatches(it) }

        log(V_VERBOSE, "Preprocessing rows for ${startingNode.tableName}")
        if (schemaGraph.table(startingNode.tableName) == null) {
            throw RuntimeException("No table '${startingNode.tableName}' in the target database")
        }
        val (name, columns, _, _) = schemaGraph.tables[startingNode.tableName]!!
        val baseInsert = "INSERT INTO $name(${columns.joinToString(",") { it.name }})\n" +
                "VALUES(${columns.joinToString(",") { "?" }})"

        val batches = startingNode.rows.chunked(args.chunkSize).map { InsertBatch(baseInsert, it) }

        return beforeBatches + batches + afterBatches
    }

    fun runBatches(batches: List<InsertBatch>) {
        conn.autoCommit = false

        var lastPcnt = 0
        try {
            batches.forEachIndexed { index, batch ->
                runBatch(batch)
                val pcnt = index * 100 / batches.size
                if (pcnt - lastPcnt > 10) {
                    log(V_NORMAL, "Inserted @|blue ${pcnt}|@%")
                    lastPcnt = pcnt
                }
            }
            log(V_NORMAL, "Inserted @|blue 100|@%")
            conn.commit()
        } catch (e: Exception) {
            conn.rollback()
            throw e
        }
        conn.autoCommit = true
    }

    private fun runBatch(batch: InsertBatch) {
        log(V_SQL, "Executing insert:\n${batch.statement}")
        val statement = conn.prepareStatement(batch.statement)
        try {
            batch.data.forEach { row ->
                // It is possible to have the same table appear multiple times in the data graph
                // this check avoids writing the same rows that were already written
                if (!writtenRows.contains(row)) {
                    row.addParametersForInsert(statement)
                    statement.addBatch()
                    writtenRows.add(row)
                }
            }
            if (!args.dryRun) {
                statement.executeBatch()
            }
        } finally {
            statement.close()
        }
    }
}

