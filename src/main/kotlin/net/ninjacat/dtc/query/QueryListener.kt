package net.ninjacat.dtc.query

import net.ninjacat.dtc.RsqlBaseListener
import net.ninjacat.dtc.RsqlParser

class QueryListener(private val src: String) : RsqlBaseListener() {

    var tableName = ""

    var filter = ""

    var distinct = false

    override fun exitSelectStatement(ctx: RsqlParser.SelectStatementContext?) {
        super.exitSelectStatement(ctx)
    }

    override fun exitWhere(ctx: RsqlParser.WhereContext?) {
        super.exitWhere(ctx)
        val start = ctx!!.expr().getStart().startIndex
        val end = ctx.expr().stop.stopIndex
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