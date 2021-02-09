package net.ninjacat.rowcp.data

import net.ninjacat.rowcp.Args
import net.ninjacat.rowcp.V_NORMAL
import net.ninjacat.rowcp.V_VERBOSE
import net.ninjacat.rowcp.log
import net.ninjacat.rowcp.query.QueryParser

class DataCopier(
    private val args: Args,
    private val parser: QueryParser,
    private val retriever: DataRetriever,
    private val mapper: DataMapper,
    private val writer: DataWriter
) {

    fun copyData() {
        log(V_NORMAL, "Starting data transfer")
        val query = parser.parseQuery(args.getQuery())
        val dataNode = retriever.collectDataToCopy(query)
        log(V_NORMAL, "Retrieved @|yellow ${dataNode.size()}|@ rows")
        log(V_NORMAL, "Mapping rows to target database")
        val mappedNode = mapper.mapToTarget(dataNode)
        val batches = writer.prepareBatches(mappedNode)
        log(V_VERBOSE, "Preparing to run @|yellow ${batches.size}|@ INSERT batches")
        writer.runBatches(batches)
        log(V_NORMAL, "@|green Done|@")
    }
}