package net.ninjacat.rowcp

import com.beust.jcommander.JCommander
import com.beust.jcommander.Parameter
import net.ninjacat.rowcp.query.ArgsParsingException
import java.io.File
import kotlin.system.exitProcess

class Args {

    @Parameter(names = ["-p", "--parameter-file"], description = "Read parameters from file")
    var paramFile: String? = null

    @Parameter(names = ["-h", "--help"], description = "Show this message")
    var showHelp: Boolean = false

    @Parameter(
        names = ["-s", "--source-connection"],
        description = "Source JDBC connection string or a file path starting with 'file:' schema"
    )
    var sourceJdbcUrl: String = ""

    @Parameter(names = ["--source-user"], description = "User name for source database")
    var sourceUser: String = ""

    @Parameter(
        names = ["--source-password"],
        description = "User password for source database",
        password = true
    )
    var sourcePassword: String = ""

    @Parameter(names = ["-t", "--target-connection"], description = "Target JDBC connection string")
    var targetJdbcUrl: String = ""

    @Parameter(names = ["--target-user"], description = "User name for target database")
    var targetUser: String = ""

    @Parameter(
        names = ["--target-password"],
        description = "User password for target database",
        password = true
    )
    var targetPassword: String = ""

    @Parameter(names = ["-v", "--verbose"], description = "Verbose output")
    var verbosity: Int = DEFAULT_VERBOSITY

    @Parameter(names = ["--chunk-size"], description = "Split long queries in chunks of that size")
    var chunkSize: Int = DEFAULT_CHUNK_SIZE

    @Parameter(names = ["-d", "--dry-run"], description = "Perform all actions except for actual data insertion")
    var dryRun: Boolean = false

    @Parameter(names = ["--skip-tables"], description = "Comma-separated list of source tables to be ignored")
    var skipSourceTables: String? = null

    @Parameter(names = ["--skip-unknown-columns"], description = "Ignore columns that do not exist in target database")
    var skipMissingColumns = false

    @Parameter(
        names = ["--update"],
        description = "Allow updating rows where primary key already exist in the target database"
    )
    var allowUpdate = false

    @Parameter(
        names = ["--show-copy-tree"],
        description = "Print list of tables that will be considered for copying"
    )
    var showTree = false

    @Parameter(
        names = ["--ignore-existing"],
        description = "Ignore existing rows during insert whenever possible. This option might not be supported with some JDBC drivers and is ignored when '--update' parameter is used"
    )
    var ignoreExisting = false

    @Parameter(description = "Query")
    var query: MutableList<String> = mutableListOf()

    val tablesToSkip: Set<String> by lazy { skipSourceTables?.split(",")?.map { it.lowercase() }?.toSet() ?: setOf() }

    private fun validate(): Args {
        if (showHelp) {
            return this
        }
        if (paramFile != null) {
            val args = loadArgsFromFile(paramFile!!)
            val mergedArgs = args.merge(this)
            return mergedArgs.validate()
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

    private fun merge(params: Args): Args {
        val result = Args()
        result.sourceJdbcUrl = override(params.sourceJdbcUrl, "", this.sourceJdbcUrl)!!
        result.sourceUser = override(params.sourceUser, "", this.sourceUser)!!
        result.sourcePassword = override(params.sourcePassword, "", this.sourcePassword)!!
        result.targetJdbcUrl = override(params.targetJdbcUrl, "", this.targetJdbcUrl)!!
        result.targetUser = override(params.targetUser, "", this.targetUser)!!
        result.targetPassword = override(params.targetPassword, "", this.targetPassword)!!
        result.verbosity = override(params.verbosity, DEFAULT_VERBOSITY, this.verbosity)!!
        result.chunkSize = override(params.chunkSize, DEFAULT_CHUNK_SIZE, this.chunkSize)!!
        result.skipSourceTables = override(params.skipSourceTables, null, this.skipSourceTables)
        result.skipMissingColumns = override(params.skipMissingColumns, false, this.skipMissingColumns)!!
        result.dryRun = override(params.dryRun, false, this.dryRun)!!
        result.query = override(params.query, mutableListOf(), this.query)!!
        result.showTree = override(params.showTree, false, this.showTree)!!
        result.ignoreExisting = override(params.ignoreExisting, false, this.ignoreExisting)!!
        result.paramFile = null
        return result
    }

    /**
     * if override value is not equal to standard than take it, otherwise use default value
     */
    private fun <T> override(override: T?, standard: T?, default: T?): T? {
        return if (override == standard) {
            default
        } else {
            override
        }
    }

    fun getQuery(): String = query.joinToString(" ").trim()

    fun nullableTargetUser(): String? = if (targetUser.isEmpty()) null else targetUser
    fun nullableTargetPassword(): String? = if (targetPassword.isEmpty()) null else targetPassword
    fun nullableSourceUser(): String? = if (sourceUser.isEmpty()) null else sourceUser
    fun nullableSourcePassword(): String? = if (sourcePassword.isEmpty()) null else sourcePassword


    companion object {
        private const val DEFAULT_CHUNK_SIZE: Int = 500
        private const val DEFAULT_VERBOSITY: Int = 1

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

        private fun loadArgsFromFile(file: String): Args {
            val lines = File(file).readLines()
            val arguments = lines.takeWhile { it.isNotEmpty() }
                .filterNot { it.trim().startsWith("#") }
                .flatMap {
                    val firstSpace = it.indexOf(' ')
                    if (firstSpace >= 0) {
                        val paramName = it.substring(0, firstSpace).trim()
                        val paramValue = it.substring(firstSpace + 1).trim()
                        listOf(paramName, paramValue)
                    } else {
                        listOf(it)
                    }
                }
            val query = lines.dropWhile { it.isNotEmpty() }
                .filter { it.isNotEmpty() }
                .filterNot { it.trim().startsWith("#") }
            val argv = (arguments + query).toTypedArray()
            val args = parse(*argv)
            args.paramFile = null
            return args
        }
    }
}