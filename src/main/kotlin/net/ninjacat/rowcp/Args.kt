package net.ninjacat.rowcp

import com.beust.jcommander.JCommander
import com.beust.jcommander.Parameter
import java.io.File
import kotlin.system.exitProcess

class ArgsParsingException : Exception("")

class Args {
    @Parameter(names = ["-p", "--parameter-file"], description = "Read parameters from file")
    var paramFile: String? = null

    @Parameter(names = ["-h", "--help"], description = "Show this mesage")
    var showHelp: Boolean = false

    @Parameter(names = ["-s", "--source-connection"], description = "Source JDBC connection string")
    var sourceJdbcUrl: String = ""

    @Parameter(names = ["--source-user"], description = "User name for source database")
    var sourceUser: String = ""

    @Parameter(
        names = ["--source-password"],
        description = "User password for source database. Not recommended to use",
        password = true
    )
    var sourcePassword: String = ""

    @Parameter(names = ["-t", "--target-connection"], description = "Target JDBC connection string")
    var targetJdbcUrl: String = ""

    @Parameter(names = ["--target-user"], description = "User name for target database")
    var targetUser: String = ""

    @Parameter(
        names = ["--target-password"],
        description = "User password for target database. Not recommended to use",
        password = true
    )
    var targetPassword: String = ""

    @Parameter(names = ["-v", "--verbose"], description = "Verbose output")
    var verbosity: Int = 1

    @Parameter(names = ["--chunk-size"], description = "Split long queries in chunks of that size")
    var chunkSize: Int = 500

    @Parameter(names = ["-d", "--dry-run"], description = "Perform all actions except for actual data insertion")
    var dryRun: Boolean = false


    @Parameter(description = "Query")
    var query: MutableList<String> = mutableListOf()


    private fun validate(): Args {
        if (showHelp) {
            return this
        }
        if (paramFile != null) {
            val lines = File(paramFile!!).readLines()
            val arguments = lines.takeWhile { it.isNotEmpty() }.flatMap {
                val firstSpace = it.indexOf(' ')
                if (firstSpace >= 0) {
                    val paramName = it.substring(0, firstSpace).trim()
                    val paramValue = it.substring(firstSpace + 1).trim()
                    listOf(paramName, paramValue)
                } else {
                    listOf(it)
                }
            }
            val query = lines.dropWhile { it.isNotEmpty() }.filter { it.isNotEmpty() }
            val argv = (arguments + query).toTypedArray()
            val args = parse(*argv)
            args.validate()
            return args
        } else {
            if (sourceJdbcUrl.isBlank()) {
                log(V_NORMAL, "@|red --source-connection|@ parameter is required")
                throw ArgsParsingException()
            }
            if (targetJdbcUrl.isBlank()) {
                log(V_NORMAL, "@|red --target-connection|@ parameter is required")
                throw ArgsParsingException()
            }
            if (query.isEmpty()) {
                log(V_NORMAL, "@|red Query|@ is required to retrieve starting rows")
                throw ArgsParsingException()
            }
        }
        return this
    }


    fun getQuery(): String = query.joinToString(" ").trim()

    fun nullableTargetUser(): String? = if (targetUser.isEmpty()) null else targetUser
    fun nullableTargetPassword(): String? = if (targetPassword.isEmpty()) null else targetPassword
    fun nullableSourceUser(): String? = if (sourceUser.isEmpty()) null else sourceUser
    fun nullableSourcePassword(): String? = if (sourcePassword.isEmpty()) null else sourcePassword


    companion object {
        fun parse(vararg argv: String): Args {
            val args = Args()
            val commander = JCommander.newBuilder()
                .addObject(args)
                .programName("rowcp")
                .build()
            commander.parse(*argv)
            try {
                val result = args.validate()
                if (result.showHelp) {
                    commander.usage()
                    exitProcess(0)
                }
                return result
            } catch (e: ArgsParsingException) {
                commander.usage()
                throw e
            }
        }
    }
}