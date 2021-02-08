package net.ninjacat.rowcp.query

import net.ninjacat.rowcp.RsqlBaseListener
import net.ninjacat.rowcp.RsqlParser

class QueryListener(private val src: String) : RsqlBaseListener() {

    var tableName = ""

    var filter = ""

    var distinct = false

    override fun exitSelectStatement(ctx: RsqlParser.SelectStatementContext?) {
        super.exitSelectStatement(ctx)
    }

    override fun exitWhere(ctx: RsqlParser.WhereContext?) {
        super.exitWhere(ctx)
        val start = ctx!!.anything().getStart().startIndex
        val end = ctx.anything().stop.stopIndex
        filter = src.substring(start, end + 1)
    }

    override fun exitSourceName(ctx: RsqlParser.SourceNameContext?) {
        super.exitSourceName(ctx)
        tableName = ctx?.name()?.text!!
    }

    override fun exitProjection(ctx: RsqlParser.ProjectionContext?) {
        super.exitProjection(ctx)
        distinct = ctx?.distinct() != null
    }
}