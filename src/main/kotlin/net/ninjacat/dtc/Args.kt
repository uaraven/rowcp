package net.ninjacat.dtc

import com.beust.jcommander.JCommander
import com.beust.jcommander.Parameter

class Args {
    @Parameter(names = ["-s", "--source-connection"], description = "Source JDBC connection string", required = true)
    var sourceJdbcUrl: String = ""

    @Parameter(names = ["--source-user"], description = "User name for source database")
    var sourceUser: String = ""

    @Parameter(
        names = ["--source-password"],
        description = "User password for source database. Not recommended to use",
        password = true
    )
    var sourcePassword: String = ""

    @Parameter(names = ["-t", "--target-connection"], description = "Target JDBC connection string", required = true)
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


    @Parameter(description = "Query", required = true)
    var query: MutableList<String> = mutableListOf()


    fun getQuery(): String = query.joinToString(" ").trim()

    fun nullableTargetUser(): String? = if (targetUser.isEmpty()) null else targetUser
    fun nullableTargetPassword(): String? = if (targetPassword.isEmpty()) null else targetPassword
    fun nullableSourceUser(): String? = if (sourceUser.isEmpty()) null else sourceUser
    fun nullableSourcePassword(): String? = if (sourcePassword.isEmpty()) null else sourcePassword


    companion object {
        fun parse(vararg argv: String): Args {
            val args = Args()
            JCommander.newBuilder().addObject(args).build().parse(*argv)
            return args
        }
    }
}