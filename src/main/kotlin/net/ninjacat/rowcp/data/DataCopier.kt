package net.ninjacat.rowcp.data

import net.ninjacat.rowcp.Args
import net.ninjacat.rowcp.V_NORMAL
import net.ninjacat.rowcp.V_VERBOSE
import net.ninjacat.rowcp.log
import net.ninjacat.rowcp.query.QueryParser

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
        val dataNode = retriever.collectDataToCopy(query, schemaGraph)
        log(V_NORMAL, "Retrieved @|yellow ${dataNode.size()}|@ rows")
        val batches = inserter.prepareBatches(dataNode, schemaGraph)
        log(V_VERBOSE, "Preparing to run @|yellow ${batches.size}|@ INSERT batches")
        inserter.runBatches(batches)
        log(V_NORMAL, "@|green Done|@")
    }
}