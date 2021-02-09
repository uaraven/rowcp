package net.ninjacat.rowcp.data

import net.ninjacat.rowcp.Args
import net.ninjacat.rowcp.query.BaseDatabaseTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Types

internal class DataWriterTest : BaseDatabaseTest() {

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
    internal fun testCreateBatches() {
        val node = prepareDataNode()

        val inserter = DataWriter(args, schema)
        val batches = inserter.prepareBatches(node)

        assertThat(batches).hasSize(4)
        assertThat(batches.flatMap { batch -> batch.data.map { it.tableName() } }).contains(
            "main",
            "intermediate",
            "intermediate_to_child",
            "child"
        )
    }

    @Test
    internal fun testChunkedBatches() {
        val node = prepareManyBatches()
        args.chunkSize = 20

        val inserter = DataWriter(args, schema)
        val batches = inserter.prepareBatches(node)

        assertThat(batches).hasSize(5)
        assertThat(batches.flatMap { batch -> batch.data.map { it.tableName() } }).contains("main")
    }

    @Test
    internal fun testInsertBatchesOneTable() {
        val node = prepareManyBatches()
        args.chunkSize = 20

        val inserter = DataWriter(args, schema)
        val batches = inserter.prepareBatches(node)
        inserter.runBatches(batches)

        val count = getRowCount("main")

        assertThat(count).isEqualTo(100)
    }

    @Test
    internal fun testInsertBatchesAllTables() {
        val node = prepareRealDataNode()

        val inserter = DataWriter(args, schema)
        val batches = inserter.prepareBatches(node)
        inserter.runBatches(batches)

        val mainCount = getRowCount("main")
        val childCount = getRowCount("child")
        val intermediateCount = getRowCount("intermediate")
        val intermediateChildCount = getRowCount("intermediate_to_child")

        assertThat(mainCount).isEqualTo(1)
        assertThat(intermediateCount).isEqualTo(1)
        assertThat(intermediateChildCount).isEqualTo(2)
        assertThat(childCount).isEqualTo(2)
    }

    private fun getRowCount(tableName: String): Int {
        val statement = targetDb.createStatement()
        val resultSet = statement.executeQuery("SELECT count(*) FROM $tableName")
        try {
            if (resultSet.next()) {
                return resultSet.getInt(1)
            }
            return 0
        } finally {
            statement.close()
            resultSet.close()
        }
    }

    private fun prepareManyBatches(): DataNode {
        val rows = (1..100).map { idx ->
            DataRow(
                graph.tables["main"]!!, listOf(
                    ColumnData("id", Types.INTEGER, idx),
                    ColumnData("text", Types.VARCHAR, "row $idx"),
                )
            )
        }
        return DataNode("main", rows, listOf(), listOf())
    }

    private fun prepareDataNode(): DataNode {
        val beforeNode = DataNode(
            "main", listOf(
                DataRow(
                    graph.tables["main"]!!, listOf(
                        ColumnData("id", Types.INTEGER, 1),
                        ColumnData("text", Types.VARCHAR, "1"),
                    )
                )
            ), listOf(), listOf()
        )
        val afterNode = DataNode(
            "intermediate", listOf(
                DataRow(
                    graph.tables["intermediate"]!!, listOf(
                        ColumnData("id", Types.INTEGER, 1),
                        ColumnData("main_id", Types.INTEGER, 1),
                        ColumnData("contents", Types.VARCHAR, "1"),
                    )
                )
            ), listOf(), listOf()
        )
        val afterNode2 = DataNode(
            "child", listOf(
                DataRow(
                    graph.table("child")!!, listOf(
                        ColumnData("first", Types.VARCHAR, "1"),
                        ColumnData("second", Types.VARCHAR, "1"),
                        ColumnData("value", Types.INTEGER, 1),
                    )
                )
            ), listOf(), listOf()
        )
        return DataNode(
            "intermediate_to_child", listOf(
                DataRow(
                    graph.table("intermediate_to_child")!!, listOf(
                        ColumnData("intermediate_id", Types.INTEGER, 1),
                        ColumnData("first_child", Types.VARCHAR, "1"),
                        ColumnData("second_child", Types.VARCHAR, "1"),
                    )
                )
            ), listOf(beforeNode), listOf(afterNode, afterNode2)
        )
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
                        ColumnData("first_child", Types.VARCHAR, "1"),
                        ColumnData("second_child", Types.VARCHAR, "1"),
                    )
                ),
                DataRow(
                    graph.table("intermediate_to_child")!!, listOf(
                        ColumnData("intermediate_id", Types.INTEGER, 1),
                        ColumnData("first_child", Types.VARCHAR, "2"),
                        ColumnData("second_child", Types.VARCHAR, "2"),
                    )
                )
            ), listOf(intermediate, child), listOf()
        )
    }
}