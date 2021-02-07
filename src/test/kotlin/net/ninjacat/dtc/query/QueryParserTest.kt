package net.ninjacat.dtc.query

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

internal class QueryParserTest {

    @Test
    internal fun testParseSimpleQuery() {
        val parser = QueryParser()
        val q = parser.parseQuery("SELECT * FROM Table")

        assertThat(q.table).isEqualTo("Table")
        assertThat(q.filter).isEqualTo("")
        assertThat(q.selectDistinct).isFalse
    }

    @Test
    internal fun testParseQueryWithWhere() {
        val parser = QueryParser()
        val q = parser.parseQuery("SELECT * FROM Table WHERE Table.A = 10 AND (b = 20 OR C LIKE 'Text%')")

        assertThat(q.table).isEqualTo("Table")
        assertThat(q.filter).isEqualTo("Table.A = 10 AND (b = 20 OR C LIKE 'Text%')")
        assertThat(q.selectDistinct).isFalse
    }

    @Test
    internal fun testParseQueryWithWhereAndDistinct() {
        val parser = QueryParser()
        val q = parser.parseQuery("SELECT distinct * FROM Table WHERE Table.A = 10 AND (b = 20 OR C LIKE 'Text%')")

        assertThat(q.table).isEqualTo("Table")
        assertThat(q.filter).isEqualTo("Table.A = 10 AND (b = 20 OR C LIKE 'Text%')")
        assertThat(q.selectDistinct).isTrue
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
    internal fun testFailWhenIncorrectWhere() {
        val parser = QueryParser()
        assertThatThrownBy {
            parser.parseQuery("SELECT * FROM Table WHERE A = ")
        }.isInstanceOf(QueryParsingException::class.java)
    }
}