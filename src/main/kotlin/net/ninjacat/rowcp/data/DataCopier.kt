package net.ninjacat.rowcp.data

import net.ninjacat.rowcp.Args
import net.ninjacat.rowcp.V_NORMAL
import net.ninjacat.rowcp.log
import net.ninjacat.rowcp.query.QueryParser

class DataCopier(
    private val args: Args,
    private val parser: QueryParser,
    private val retriever: DataRetriever,
    private val mapper: Mapper,
    private val writer: DataWriter
) {

    fun copyData() {
        log(V_NORMAL, "Starting data transfer")
        val queries = parser.parseQuery(args.getQuery())
        queries.forEach { query ->
            log(V_NORMAL, "Seed query @|blue ${query.text}|@")
            val dataNode = retriever.collectDataToCopy(query)
            log(V_NORMAL, "Retrieved @|yellow ${dataNode.size()}|@ rows")
            log(V_NORMAL, "Mapping rows to target database")
            val mappedNode = mapper.mapToTarget(dataNode)
            writer.writeData(mappedNode)
            log(V_NORMAL, "@|green Done|@")
        }
    }
}