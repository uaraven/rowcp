package net.ninjacat.rowcp

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import net.ninjacat.rowcp.data.*
import net.ninjacat.rowcp.data.export.SqlMapper
import net.ninjacat.rowcp.data.export.SqlWriter
import net.ninjacat.rowcp.data.visualizer.CopyVisualizer
import net.ninjacat.rowcp.query.ArgsParsingException
import net.ninjacat.rowcp.query.QueryParser
import org.fusesource.jansi.AnsiConsole
import org.slf4j.LoggerFactory

fun main(vararg argv: String) {
    // disable logging
    val rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as Logger
    rootLogger.level = Level.OFF

    AnsiConsole.systemInstall()

    try {
        val args = Args.parse(*argv)

        currentLogLevel = args.verbosity

        val parser = QueryParser()

        Utils.initializeDatabase(args.sourceJdbcUrl)

        if (args.dryRun) {
            log(V_NORMAL, "Performing a @|blue dry run|@")
        }

        val sourceDbSchema = DbSchema(args.sourceJdbcUrl, args.nullableSourceUser(), args.nullableSourcePassword())
        if (args.showTree) {
            val visualizer = CopyVisualizer(args, parser, sourceDbSchema)
            visualizer.showCopyTree()
        } else {
            Utils.initializeDatabase(args.targetJdbcUrl)

            log(V_NORMAL, "Copying rows from @|blue ${args.sourceJdbcUrl}|@ to @|cyan ${args.targetJdbcUrl}|@")

            val retriever = DataRetriever(args, sourceDbSchema)

            val (mapper, writer: DataWriter) = if (args.targetJdbcUrl.startsWith("file:")) {
                getTargetFileProcessors(args)
            } else {
                getTargetJdbcProcessors(args)
            }

            val copier = DataCopier(args, parser, retriever, mapper, writer)
            copier.copyData()
        }
    } catch (ae: ArgsParsingException) {
        // do nothing, help has been shown already
    } catch (e: Exception) {
        logError(e, e.message)
    }

    AnsiConsole.systemUninstall()
}

private fun getTargetJdbcProcessors(args: Args): Pair<Mapper, DataWriter> {
    log(V_NORMAL, "Copying @|blue JDBC|@ target")
    val targetDbSchema = DbSchema(args.targetJdbcUrl, args.nullableTargetUser(), args.nullableTargetPassword())
    val mapper = DataMapper(args, targetDbSchema)
    val writer: DataWriter = if (args.allowUpdate) {
        DataUpdater(args, targetDbSchema)
    } else {
        DataInserter(args, targetDbSchema)
    }
    return Pair(mapper, writer)
}

private fun getTargetFileProcessors(args: Args): Pair<Mapper, DataWriter> {
    log(V_NORMAL, "Copying @|blue SQL file|@ target")
    val mapper = SqlMapper()
    val writer = SqlWriter(args.targetJdbcUrl)
    return Pair(mapper, writer)
}
