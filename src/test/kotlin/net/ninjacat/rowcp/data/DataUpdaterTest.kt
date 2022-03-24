package net.ninjacat.rowcp.data

import net.ninjacat.rowcp.Args
import net.ninjacat.rowcp.data.Utils.use
import net.ninjacat.rowcp.query.BaseDatabaseTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Connection
import java.sql.Timestamp
import java.sql.Types
import java.time.LocalDateTime

internal class DataUpdaterTest : BaseDatabaseTest() {

    private lateinit var graph: SchemaGraph
    private lateinit var schema: DbSchema
    private lateinit var args: Args

    @BeforeEach
    override fun setUp() {
        super.setUp()
        this.args = createArgs(sourceUrl, targetUrl)
        this.schema = DbSchema(targetUrl, null, null)
        this.graph = schema.getSchemaGraph()
    }

    @Test
    internal fun testCreateUpdatesNoData() {
        val node = prepareRealDataNode()

        val updater = DataUpdater(args, schema)
        val updates = updater.prepareUpdates(node)

        assertThat(updates).hasSize(6)
        assertThat(updates.map { update -> update.data.tableName() }).contains(
            "main",
            "intermediate",
            "intermediate_to_child",
            "child"
        )
        assertThat(updates.map { update -> update.statement.substringBefore(' ') }.toSet())
            .contains("INSERT")
    }

    @Test
    internal fun testCreateUpdates() {
        val node = prepareRealDataNode()

        insertExistingData(schema.connection)

        val updater = DataUpdater(args, schema)
        val updates = updater.prepareUpdates(node)

        assertThat(updates.map { update -> update.statement.substringBefore(' ') }.toSet())
            .contains("INSERT", "UPDATE")
    }

    @Test
    internal fun testRunUpdates() {
        val node = prepareRealDataNode()

        insertExistingData(schema.connection)

        val updater = DataUpdater(args, schema)
        updater.writeData(node)

        val main = readValues(schema.connection, "SELECT text FROM main")
        assertThat(main).containsExactlyInAnyOrder("1")

        val children = readValues(schema.connection, "SELECT value FROM child")
        assertThat(children).containsExactlyInAnyOrder(1, 2, 15)
    }

    private fun insertExistingData(connection: Connection) {
        connection.prepareStatement("INSERT INTO child(first, second, value, updated_on) VALUES (?, ?, ?, ?)").use {
            setString(1, "1")
            setString(2, "1")
            setInt(3, 11)
            setTimestamp(4, Timestamp.valueOf(LocalDateTime.now()))
            addBatch()
            setString(1, "2")
            setString(2, "2")
            setInt(3, 12)
            setTimestamp(4, Timestamp.valueOf(LocalDateTime.now()))
            addBatch()
            setString(1, "5")
            setString(2, "5")
            setInt(3, 15)
            setTimestamp(4, Timestamp.valueOf(LocalDateTime.now()))
            addBatch()

            executeBatch()
        }
    }


    private fun prepareRealDataNode(): DataNode {
        val main = DataNode(
            "main", listOf(
                DataRow(
                    graph.table("main")!!, listOf(
                        ColumnData("id", Types.INTEGER, 1),
                        ColumnData("text", Types.VARCHAR, "1"),
                    )
                )
            ), listOf(), listOf()
        )
        val intermediate = DataNode(
            "intermediate", listOf(
                DataRow(
                    graph.table("intermediate")!!, listOf(
                        ColumnData("id", Types.INTEGER, 1),
                        ColumnData("main_id", Types.INTEGER, 1),
                        ColumnData("contents", Types.VARCHAR, "1"),
                        ColumnData("time", Types.TIMESTAMP, "2020-01-01T10:11:12")
                    )
                )
            ), listOf(main), listOf()
        )
        val child = DataNode(
            "child", listOf(
                DataRow(
                    graph.table("child")!!, listOf(
                        ColumnData("first", Types.VARCHAR, "1"),
                        ColumnData("second", Types.VARCHAR, "1"),
                        ColumnData("value", Types.INTEGER, 1),
                        ColumnData("updated_on", Types.TIMESTAMP, "2020-01-01T10:11:12")
                    )
                ),
                DataRow(
                    graph.table("child")!!, listOf(
                        ColumnData("first", Types.VARCHAR, "2"),
                        ColumnData("second", Types.VARCHAR, "2"),
                        ColumnData("value", Types.INTEGER, 2),
                        ColumnData("updated_on", Types.TIMESTAMP, "2020-01-01T10:11:13")
                    )
                )
            ), listOf(), listOf()
        )
        return DataNode(
            "intermediate_to_child", listOf(
                DataRow(
                    graph.table("intermediate_to_child")!!, listOf(
                        ColumnData("intermediate_id", Types.INTEGER, 1),
                        ColumnData("child_first", Types.VARCHAR, "1"),
                        ColumnData("child_first", Types.VARCHAR, "1"),
                    )
                ),
                DataRow(
                    graph.table("intermediate_to_child")!!, listOf(
                        ColumnData("intermediate_id", Types.INTEGER, 1),
                        ColumnData("child_first", Types.VARCHAR, "2"),
                        ColumnData("child_first", Types.VARCHAR, "2"),
                    )
                )
            ), listOf(intermediate, child), listOf()
        )
    }
}
