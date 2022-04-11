package net.ninjacat.rowcp.data

import net.ninjacat.rowcp.query.BaseDatabaseTest
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class DbSchemaTest : BaseDatabaseTest() {

    @Test
    internal fun testSchemaGraphContainsTables() {
        val args = createArgs(sourceUrl, targetUrl)

        val schema = DbSchema("test", args.sourceJdbcUrl, args.nullableSourceUser(), args.nullableSourcePassword())

        val graph = schema.getSchemaGraph()

        assertThat(graph.tables).containsKeys("main", "intermediate", "child", "intermediate_to_child")

        assertThat(graph.table("MAIN")!!.columns).extracting<String> { c -> c.name }
            .contains("id", "text")
    }

    @Test
    internal fun testOutboundForeignKeys() {
        val args = createArgs(sourceUrl, targetUrl)

        val schema = DbSchema("test", args.sourceJdbcUrl, args.nullableSourceUser(), args.nullableSourcePassword())

        val graph = schema.getSchemaGraph()
        val child = graph.table("CHILD")!!
        assertThat(child.outbound).hasSize(1)
        val relation = child.outbound.first()

        assertThat(relation.targetTable).isEqualTo("intermediate_to_child")
        assertThat(relation.sourceTable).isEqualTo("child")
        assertThat(relation.columnMap).extracting<String> { cm -> cm.sourceColumn + "-" + cm.targetColumn }
            .contains("first-child_first", "second-child_second")
    }

    @Test
    internal fun testInboundForeignKeys() {
        val args = createArgs(sourceUrl, targetUrl)

        val schema = DbSchema("test", args.sourceJdbcUrl, args.nullableSourceUser(), args.nullableSourcePassword())

        val graph = schema.getSchemaGraph()
        val table = graph.table("intermediate_to_child")!!
        assertThat(table.inbound).hasSize(2)
        val relation = table.inbound.find { it.sourceTable == "child" }!!

        assertThat(relation.targetTable).isEqualToIgnoringCase("intermediate_to_child")
        assertThat(relation.columnMap).extracting<String> { cm -> cm.sourceColumn + "-" + cm.targetColumn }
            .contains("first-child_first", "second-child_second")
    }

}