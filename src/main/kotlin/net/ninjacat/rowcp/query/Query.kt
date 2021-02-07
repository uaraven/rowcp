package net.ninjacat.rowcp.query

data class Query(val table: String, val filter: String, val selectDistinct: Boolean)