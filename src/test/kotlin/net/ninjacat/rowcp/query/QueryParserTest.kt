package net.ninjacat.rowcp.query

import org.assertj.core.api.Assertions.*
import org.junit.jupiter.api.Test

internal class QueryParserTest {

    @Test
    internal fun testParseSimpleQuery() {
        val parser = QueryParser()
        val ql = parser.parseQuery("SELECT * FROM Table")

        assertThat(ql).hasSize(1)
        val q = ql[0]
        assertThat(q.table).isEqualTo("Table")
        assertThat(q.filter).isEqualTo("")
        assertThat(q.selectDistinct).isFalse
    }

    @Test
    internal fun testParseQueryWithWhere() {
        val parser = QueryParser()
        val ql = parser.parseQuery("SELECT * FROM Table WHERE Table.A = 10 AND (b = 20 OR C LIKE 'Text%')")

        assertThat(ql).hasSize(1)
        val q = ql[0]

        assertThat(q.table).isEqualTo("Table")
        assertThat(q.filter).isEqualTo("Table.A = 10 AND (b = 20 OR C LIKE 'Text%')")
        assertThat(q.selectDistinct).isFalse
    }

    @Test
    internal fun testParseQueryWithWhereAndDistinct() {
        val parser = QueryParser()
        val ql = parser.parseQuery("SELECT distinct * FROM Table WHERE Table.A = 10 AND (b = 20 OR C LIKE 'Text%')")

        assertThat(ql).hasSize(1)
        val q = ql[0]

        assertThat(q.table).isEqualTo("Table")
        assertThat(q.filter).isEqualTo("Table.A = 10 AND (b = 20 OR C LIKE 'Text%')")
        assertThat(q.selectDistinct).isTrue
    }

    @Test
    internal fun testParseQueryWithTableAlias() {
        val parser = QueryParser()
        val ql = parser.parseQuery("SELECT distinct * FROM Table T1 WHERE T1.A = 10 AND (b = 20 OR C LIKE 'Text%')")

        assertThat(ql).hasSize(1)
        val q = ql[0]

        assertThat(q.table).isEqualTo("Table")
        assertThat(q.alias).isEqualTo("T1")
        assertThat(q.filter).isEqualTo("T1.A = 10 AND (b = 20 OR C LIKE 'Text%')")
        assertThat(q.selectDistinct).isTrue
    }

    @Test
    internal fun testParseMultipleQueries() {
        val parser = QueryParser()
        val ql = parser.parseQuery(
            "SELECT * FROM Table WHERE Table.A = 10 AND (b = 20 OR C LIKE 'Text%');\n"
                    + "SELECT * FROM Table2 T2 WHERE T2.Age < 80\n"
        )

        assertThat(ql).hasSize(2)
        val q1 = ql[0]

        assertThat(q1.table).isEqualTo("Table")
        assertThat(q1.filter).isEqualTo("Table.A = 10 AND (b = 20 OR C LIKE 'Text%')")
        assertThat(q1.selectDistinct).isFalse

        val q2 = ql[1]

        assertThat(q2.table).isEqualTo("Table2")
        assertThat(q2.filter).isEqualTo("T2.Age < 80")
        assertThat(q2.selectDistinct).isFalse
    }

    @Test
    internal fun testFailWhenProjectionIsNotAll() {
        val parser = QueryParser()
        assertThatThrownBy {
            parser.parseQuery("SELECT A, B FROM Table WHERE Table.A = 10 AND (b = 20 OR C LIKE 'Text%')")
        }.isInstanceOf(QueryParsingException::class.java)
    }

    @Test
    internal fun testFailWhenMultipleTables() {
        val parser = QueryParser()
        assertThatThrownBy {
            parser.parseQuery("SELECT * FROM Table, Table2")
        }.isInstanceOf(QueryParsingException::class.java)
    }

    @Test
    internal fun testFailWhenMultipleTablesWithAliases() {
        val parser = QueryParser()
        assertThatThrownBy {
            parser.parseQuery("SELECT * FROM Table T1, Table2 T2")
        }.isInstanceOf(QueryParsingException::class.java)
    }

    @Test
    internal fun testNotFailWhenIncorrectWhere() {
        val parser = QueryParser()
        assertThatCode {
            parser.parseQuery("SELECT * FROM Table WHERE A = ")
        }.doesNotThrowAnyException()
    }
}