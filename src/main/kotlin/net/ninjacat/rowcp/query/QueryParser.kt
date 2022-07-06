package net.ninjacat.rowcp.query

import net.ninjacat.rowcp.RsqlLexer
import net.ninjacat.rowcp.RsqlParser
import org.antlr.v4.runtime.*
import org.antlr.v4.runtime.atn.ATNConfigSet
import org.antlr.v4.runtime.dfa.DFA
import org.antlr.v4.runtime.tree.ParseTreeWalker
import java.util.*

data class ParserError(val line: Int, val pos: Int, val msg: String, val e: Exception)

class QueryParser : ANTLRErrorListener {

    private val errors: MutableList<ParserError> = mutableListOf()

    fun parseQuery(query: String): List<Query> {
        val stream = CharStreams.fromString(query)
        val lexer = RsqlLexer(stream)
        val parser = RsqlParser(CommonTokenStream(lexer))
        parser.addErrorListener(this)
        errors.clear()

        val result = mutableListOf<Query>()
        val q = parser.query()

        if (errors.isNotEmpty()) {
            val sb = StringBuilder()
            with(sb) {
                errors.forEach {
                    val msg = "[${it.line}:${it.pos}] ${it.msg}"
                    append(msg).append("\n")
                }
            }
            throw QueryParsingException(sb.toString())
        }

        val selects = q.selectStatement()

        selects.forEach { select ->
            val listener = QueryListener(query)
            val text = query.substring(select.start.startIndex, select.stop.stopIndex + 1)

            ParseTreeWalker.DEFAULT.walk(listener, select)
            result.add(Query(text, listener.tableName, listener.tableAlias, listener.filter, listener.distinct))
        }

        return result.toList()
    }

    override fun syntaxError(
        recognizer: Recognizer<*, *>?,
        offendingSymbol: Any?,
        line: Int,
        charPositionInLine: Int,
        msg: String?,
        e: RecognitionException?
    ) {
        errors.add(ParserError(line, charPositionInLine, msg!!, e!!))
    }

    override fun reportAmbiguity(
        recognizer: Parser?,
        dfa: DFA?,
        startIndex: Int,
        stopIndex: Int,
        exact: Boolean,
        ambigAlts: BitSet?,
        configs: ATNConfigSet?
    ) {
    }

    override fun reportAttemptingFullContext(
        recognizer: Parser?,
        dfa: DFA?,
        startIndex: Int,
        stopIndex: Int,
        conflictingAlts: BitSet?,
        configs: ATNConfigSet?
    ) {
    }

    override fun reportContextSensitivity(
        recognizer: Parser?,
        dfa: DFA?,
        startIndex: Int,
        stopIndex: Int,
        prediction: Int,
        configs: ATNConfigSet?
    ) {
    }
}
