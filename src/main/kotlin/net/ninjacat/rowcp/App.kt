package net.ninjacat.rowcp

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import net.ninjacat.rowcp.data.*
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
        Utils.initializeDatabase(args.targetJdbcUrl)

        log(V_NORMAL, "Copying rows from @|blue ${args.sourceJdbcUrl}|@ to @|cyan ${args.targetJdbcUrl}|@")
        if (args.dryRun) {
            log(V_NORMAL, "Performing a @|blue dry run|@")
        }

        val sourceDbSchema = DbSchema(args.sourceJdbcUrl, args.nullableSourceUser(), args.nullableSourcePassword())
        val targetDbSchema = DbSchema(args.targetJdbcUrl, args.nullableTargetUser(), args.nullableTargetPassword())
        val retriever = DataRetriever(args, sourceDbSchema)
        val mapper = DataMapper(args, targetDbSchema)
        val writer: DataWriter = if (args.allowUpdate) {
            DataUpdater(args, targetDbSchema)
        } else {
            DataInserter(args, targetDbSchema)
        }

        val copier = DataCopier(args, parser, retriever, mapper, writer)
        copier.copyData()

    } catch (ae: ArgsParsingException) {
        // do nothing, help has been shown already
    } catch (e: Exception) {
        logError(e, e.message)
    }

    AnsiConsole.systemUninstall()
}
