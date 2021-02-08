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

    val args = Args.parse(*argv)

    currentLogLevel = args.verbosity

    AnsiConsole.systemInstall()

    try {
        val parser = QueryParser()

        utils.initializeDatabase(args.sourceJdbcUrl)
        utils.initializeDatabase(args.targetJdbcUrl)

        val dbSchema = DbSchema(args)
        val retriever = DataRetriever(args, dbSchema)
        val inserter = DataInserter(args)

        val copier = DataCopier(args, parser, dbSchema, retriever, inserter)
        copier.copyData()

    } catch (ae: ArgsParsingException) {
        // do nothing, help has been shown already
    } catch (e: Exception) {
        logError(e.message)
    }

    AnsiConsole.systemUninstall()
}
