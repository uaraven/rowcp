package net.ninjacat.rowcp.data

import net.ninjacat.rowcp.Args
import net.ninjacat.rowcp.query.BaseDatabaseTest
import net.ninjacat.rowcp.query.QueryParser
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

internal class DataRetrieverTest : BaseDatabaseTest() {

    private lateinit var schema: DbSchema
    private lateinit var args: Args

    @BeforeEach
    override fun setUp() {
        super.setUp()
        this.args = createArgs(sourceUrl, targetUrl)
        this.schema = DbSchema("test", sourceUrl, null, null)
    }

    @Test
    internal fun testRetrieveFromTop() {
        val dataRetriever = DataRetriever(args, schema)

        val query = QueryParser().parseQuery("SELECT * FROM main WHERE id = 1")[0]

        val data = dataRetriever.collectDataToCopy(query)

        assertThat(data.rows).extracting<List<Any?>> { it.dataOnly() }.contains(listOf(1, "text 1"))

        val intermediateData = findTableDataInNode("intermediate", data)
        assertThat(intermediateData).extracting<List<Any?>> { it.dataOnly().subList(0, 3) }.contains(
            listOf(1, 1, "content 1"),
            listOf(2, 1, "content 2")
        )

        val intermediateChildData = findTableDataInNode("intermediate_to_child", data)
        assertThat(intermediateChildData).extracting<List<Any?>> { it.dataOnly() }.contains(
            listOf(1, "first 1", "second 1"),
            listOf(2, "first 2", "second 2")
        )

        val childData = findTableDataInNode("child", data)
        assertThat(childData).extracting<List<Any?>> { it.dataOnly().subList(0, 3) }.contains(
            listOf("first 1", "second 1", 1),
            listOf("first 2", "second 2", 2)
        )

        val subMainData = findTableDataInNode("sub_main", data)
        assertThat(subMainData).extracting<List<Any?>> { it.dataOnly().subList(0, 3) }.contains(
            listOf(1, "k1", 1)
        )

        val parentData = findTableDataInNode("sub_main_rel", data)
        assertThat(parentData).extracting<List<Any?>> { it.dataOnly() }.isNotEmpty
    }

    @Test
    internal fun testRetrieveFromBottom() {
        val dataRetriever = DataRetriever(args, schema)

        val query = QueryParser().parseQuery("SELECT * FROM child WHERE first = 'first 4'")[0]

        val data = dataRetriever.collectDataToCopy(query)

        assertThat(data.rows).extracting<List<Any?>> { it.dataOnly().subList(0, 3) }.contains(
            listOf("first 4", "second 4", 4)
        )

        val intermediateData = findTableDataInNode("intermediate", data)
        assertThat(intermediateData).extracting<List<Any?>> { it.dataOnly().subList(0, 3) }.contains(
            listOf(4, 3, "content 4")
        )

        val intermediateChildData = findTableDataInNode("intermediate_to_child", data)
        assertThat(intermediateChildData).extracting<List<Any?>> { it.dataOnly() }.contains(
            listOf(4, "first 4", "second 4"),
        )

        val childData = findTableDataInNode("main", data)
        assertThat(childData).extracting<List<Any?>> { it.dataOnly() }.contains(
            listOf(3, "text 3")
        )

        val subMainData = findTableDataInNode("sub_main", data)
        assertThat(subMainData).extracting<List<Any?>> { it.dataOnly() }.isEmpty()

        val parentData = findTableDataInNode("sub_main_rel", data)
        assertThat(parentData).extracting<List<Any?>> { it.dataOnly() }.isEmpty()
    }

    @Test
    internal fun testRetrieveFromMiddle() {
        val dataRetriever = DataRetriever(args, schema)

        val query = QueryParser().parseQuery("SELECT * FROM intermediate WHERE id = 4")[0]

        val data = dataRetriever.collectDataToCopy(query)

        assertThat(data.rows).extracting<List<Any?>> { it.dataOnly().subList(0, 3) }.contains(
            listOf(4, 3, "content 4")
        )

        val intermediateData = findTableDataInNode("child", data)
        assertThat(intermediateData).extracting<List<Any?>> { it.dataOnly().subList(0, 3) }.contains(
            listOf("first 4", "second 4", 4),
            listOf("first 5", "second 5", 5)
        )

        val intermediateChildData = findTableDataInNode("intermediate_to_child", data)
        assertThat(intermediateChildData).extracting<List<Any?>> { it.dataOnly() }.contains(
            listOf(4, "first 4", "second 4"),
            listOf(4, "first 5", "second 5"),
        )

        val childData = findTableDataInNode("main", data)
        assertThat(childData).extracting<List<Any?>> { it.dataOnly() }.contains(
            listOf(3, "text 3")
        )

        val subMainData = findTableDataInNode("sub_main", data)
        assertThat(subMainData).extracting<List<Any?>> { it.dataOnly() }.isEmpty()

        val parentData = findTableDataInNode("sub_main_rel", data)
        assertThat(parentData).extracting<List<Any?>> { it.dataOnly() }.isEmpty()
    }

    private fun findTableDataInNode(tableName: String, node: DataNode): List<DataRow> {
        return node.rows.filter { it.tableName() == tableName } +
                node.before.flatMap { findTableDataInNode(tableName, it) } +
                node.after.flatMap { findTableDataInNode(tableName, it) }
    }

}