package net.ninjacat.dtc.data

import net.ninjacat.dtc.Args
import net.ninjacat.dtc.V_NORMAL
import net.ninjacat.dtc.V_VERBOSE
import net.ninjacat.dtc.log
import net.ninjacat.dtc.query.QueryParser

class DataCopier(
    private val args: Args,
    private val parser: QueryParser,
    private val dbSchema: DbSchema,
    private val retriever: DataRetriever,
    private val inserter: DataInserter
) {

    fun copyData() {
        log(V_NORMAL, "Starting data transfer")
        val query = parser.parseQuery(args.getQuery())
        val schemaGraph = dbSchema.buildSchemaGraph()
        val rows = retriever.collectDataToCopy(query, schemaGraph)
        log(V_NORMAL, "Retrieved @|yellow ${rows.size}|@ rows")
        val batches = inserter.prepareBatches(rows, schemaGraph)
        log(V_VERBOSE, "Preparing to run @|yellow ${batches.size}|@ INSERT batches")
        inserter.runBatches(batches)
        log(V_NORMAL, "@|green Done|@")
    }
}