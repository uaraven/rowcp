package net.ninjacat.rowcp.query

data class Query(val table: String, val alias: String?, val filter: String, val selectDistinct: Boolean) {
    /**
     * Returns table name with optional alias
     */
    fun tableName(): String = if (alias == null) table else "$table $alias"
}