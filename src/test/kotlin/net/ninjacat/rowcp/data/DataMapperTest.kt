package net.ninjacat.rowcp.data

import io.mockk.every
import io.mockk.mockk
import net.ninjacat.rowcp.Args
import net.ninjacat.rowcp.query.BaseDatabaseTest
import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.sql.Types

internal class DataMapperTest : BaseDatabaseTest() {

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
    internal fun testOneToOneMapping() {
        val row = DataRow(
            graph.table("main")!!, listOf(
                ColumnData("id", Types.INTEGER, 1),
                ColumnData("text", Types.VARCHAR, "1")
            )
        )
        val node = DataNode(
            "main", listOf(row), listOf(), listOf()
        )
        val mapper = DataMapper(args, schema)
        val mappedNode = mapper.mapToTarget(node)

        assertThat(mappedNode).isEqualTo(node)
    }

    @Test
    internal fun testMissingTargetTable() {
        val mockGraph = makeFakeGraph("main1", listOf(Column("id", Types.INTEGER), Column("text", Types.VARCHAR)))
        val mockTargetSchema = mockk<DbSchema>()
        every { mockTargetSchema.getSchemaGraph() } returns mockGraph

        val row = DataRow(
            graph.table("main")!!, listOf(
                ColumnData("id", Types.INTEGER, 1),
                ColumnData("text", Types.VARCHAR, "1")
            )
        )
        val node = DataNode(
            "main1", listOf(row), listOf(), listOf()
        )
        val mapper = DataMapper(args, mockTargetSchema)

        Assertions.assertThatThrownBy {
            mapper.mapToTarget(node)
        }.isInstanceOf(ValidationException::class.java)
            .hasMessageContaining("does not exist in the target database")
    }

    @Test
    internal fun testExtraColumnInSourceData_NotAllowed() {
        val mockGraph = makeFakeGraph(
            "main",
            listOf(
                Column("id", Types.INTEGER), Column("text", Types.VARCHAR)
            )
        )
        val mockTargetSchema = mockk<DbSchema>()
        every { mockTargetSchema.getSchemaGraph() } returns mockGraph

        val row = DataRow(
            graph.table("main")!!, listOf(
                ColumnData("id", Types.INTEGER, 1),
                ColumnData("text", Types.VARCHAR, "1"),
                ColumnData("extra", Types.INTEGER, 2)
            )
        )
        val node = DataNode(
            "main1", listOf(row), listOf(), listOf()
        )
        val mapper = DataMapper(args, mockTargetSchema)

        Assertions.assertThatThrownBy {
            mapper.mapToTarget(node)
        }.isInstanceOf(ValidationException::class.java)
            .hasMessageContaining("not exist in table main in the target database")
    }

    @Test
    internal fun testExtraColumnInSourceData_Allowed() {
        args.skipMissingColumns = true
        val targetGraph = makeFakeGraph(
            "main",
            listOf(
                Column("id", Types.INTEGER), Column("text", Types.VARCHAR)
            )
        )
        val mockTargetSchema = mockk<DbSchema>()
        every { mockTargetSchema.getSchemaGraph() } returns targetGraph

        val row = DataRow(
            graph.table("main")!!, listOf(
                ColumnData("id", Types.INTEGER, 1),
                ColumnData("text", Types.VARCHAR, "1"),
                ColumnData("extra", Types.INTEGER, 2)
            )
        )
        val node = DataNode(
            "main1", listOf(row), listOf(), listOf()
        )
        val mapper = DataMapper(args, mockTargetSchema)

        val mapped = mapper.mapToTarget(node)
        assertThat(mapped.rows.first().columns).hasSize(2).extracting("columnName")
            .containsExactlyInAnyOrder("id", "text")
    }

    @Test
    internal fun testNotEnoughColumnsInSourceData() {
        val mockGraph = makeFakeGraph(
            "main",
            listOf(
                Column("id", Types.INTEGER), Column("text", Types.VARCHAR), Column("extra", Types.INTEGER)
            )
        )
        val mockTargetSchema = mockk<DbSchema>()
        every { mockTargetSchema.getSchemaGraph() } returns mockGraph

        val sourceRow = DataRow(
            graph.table("main")!!, listOf(
                ColumnData("id", Types.INTEGER, 1),
                ColumnData("text", Types.VARCHAR, "1"),
            )
        )
        val sourceNode = DataNode(
            "main1", listOf(sourceRow), listOf(), listOf()
        )
        val mapper = DataMapper(args, mockTargetSchema)

        Assertions.assertThatThrownBy {
            mapper.mapToTarget(sourceNode)
        }.isInstanceOf(ValidationException::class.java)
            .hasMessageContaining("not exist in table main in the source database")
    }

    private fun makeFakeGraph(tableName: String, columns: List<Column>): SchemaGraph {
        val main1Table = Table(
            tableName,
            columns,
            setOf(),
            setOf(),
            setOf("id")
        )
        return SchemaGraph(mapOf(tableName to main1Table))
    }
}