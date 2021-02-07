package net.ninjacat.rowcp

import com.beust.jcommander.JCommander
import com.beust.jcommander.Parameter
import java.io.File
import kotlin.system.exitProcess

class Args {
    @Parameter(names = ["-p", "--param-file"], description = "Read parameters from file")
    var paramFile: String? = null


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


    @Parameter(description = "Query", descriptionKey = "seed query")
    var query: MutableList<String> = mutableListOf()


    private fun validate(commander: JCommander) {
        if (paramFile != null) {
            val lines = File(paramFile!!).readLines()
            val arguments = lines.takeWhile { it.isNotEmpty() }.toTypedArray()
            val query = lines.dropWhile { it.isNotEmpty() }
            val args = parse(*arguments)
            args.paramFile = null // don't allow recursion
            args.query.addAll(query)
            args.validate(commander)
        } else {
            var err = false
            if (sourceJdbcUrl.isBlank()) {
                log(V_NORMAL, "@|red --source-connection|@ parameter is required")
                err = true
            }
            if (targetJdbcUrl.isBlank()) {
                log(V_NORMAL, "@|red --target-connection|@ parameter is required")
                err = true
            }
            if (query.isEmpty()) {
                log(V_NORMAL, "@|red seed query|@  is required")
                err = true
            }
            if (err) {
                commander.usage()
                exitProcess(-1)
            }
        }
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
            args.validate(commander)
            return args
        }
    }
}