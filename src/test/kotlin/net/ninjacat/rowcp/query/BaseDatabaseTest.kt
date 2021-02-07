package net.ninjacat.rowcp.query

import liquibase.Contexts
import liquibase.LabelExpression
import liquibase.database.DatabaseFactory
import liquibase.database.jvm.JdbcConnection
import liquibase.resource.ClassLoaderResourceAccessor
import net.ninjacat.rowcp.Args
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import java.sql.Connection
import java.sql.DriverManager

open class BaseDatabaseTest {

    lateinit var sourceDb: Connection
    lateinit var targetDb: Connection

    @BeforeEach
    internal open fun setUp() {
        initDatabases()
        cleanupTables(targetDb, "intermediate_to_child", "child", "intermediate", "main")
    }

    @AfterEach
    internal fun tearDown() {
        targetDb.createStatement().executeUpdate("DROP ALL OBJECTS")
        sourceDb.close()
        targetDb.close()
    }

    fun createArgs(sourceUrl: String, targetUrl: String): Args {
        val args = Args()
        args.sourceJdbcUrl = sourceUrl
        args.targetJdbcUrl = targetUrl
        return args
    }

    private fun initDatabases() {
        Class.forName("org.h2.Driver")
        sourceDb = DriverManager.getConnection(sourceUrl)!!
        targetDb = DriverManager.getConnection(targetUrl)!!

        sourceDb.autoCommit = true
        targetDb.autoCommit = true

        runLiquibase(sourceDb, "/liquibase/source-changeset.xml")
        runLiquibase(targetDb, "/liquibase/target-changeset.xml")
    }

    private fun cleanupTables(targetDb: Connection, vararg tables: String) {
        tables.forEach {
            val statement = targetDb.createStatement()
            try {
                statement.executeUpdate("DELETE FROM $it")
            } finally {
                statement.close()
            }
        }
    }

    private fun runLiquibase(connection: Connection, changeLogPath: String) {
        val database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(JdbcConnection(connection))
        val migrator = liquibase.Liquibase(changeLogPath, ClassLoaderResourceAccessor(), database)
        migrator.update(Contexts(), LabelExpression())
    }

    companion object {
        val sourceUrl = "jdbc:h2:mem:src"
        val targetUrl = "jdbc:h2:mem:target"
    }
}