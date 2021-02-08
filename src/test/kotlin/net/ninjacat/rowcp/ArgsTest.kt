package net.ninjacat.rowcp

import org.assertj.core.api.Assertions
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

internal class ArgsTest {

    @Test
    internal fun testRequireSourceUrl() {
        Assertions.assertThatThrownBy {
            Args.parse("--target-connection", "jdbc-url", "select * from something")
        }.isInstanceOf(ArgsParsingException::class.java)
    }

    @Test
    internal fun testRequireTargetUrl() {
        Assertions.assertThatThrownBy {
            Args.parse("--source-connection", "jdbc-url", "select * from something")
        }.isInstanceOf(ArgsParsingException::class.java)
    }

    @Test
    internal fun testRequireQuery() {
        Assertions.assertThatThrownBy {
            Args.parse("--source-connection", "jdbc-url", "--target-connection", "also-jdbc-url")
        }.isInstanceOf(ArgsParsingException::class.java)
    }

    @Test
    internal fun testParameterFile() {
        val args = Args.parse("--parameter-file", "test-data/parameters.txt")

        assertThat(args.sourceJdbcUrl).isEqualTo("jdbc:h2:mem:source")
        assertThat(args.sourceUser).isEqualTo("src")
        assertThat(args.sourcePassword).isEqualTo("srcp")
        assertThat(args.targetJdbcUrl).isEqualTo("jdbc:h2:mem:target")
        assertThat(args.targetUser).isEqualTo("tgt")
        assertThat(args.targetPassword).isEqualTo("tgtp")
        assertThat(args.chunkSize).isEqualTo(100)
        assertThat(args.dryRun).isTrue
        assertThat(args.verbosity).isEqualTo(2)
        assertThat(args.paramFile).isNull()

        assertThat(args.query).isEqualTo(
            listOf(
                "SELECT * FROM Table WHERE it > that",
                "AND that LIKE 'Stuff%'"
            )
        )
    }
}