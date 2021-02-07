package net.ninjacat.dtc.query

data class Query(val table: String, val filter: String, val selectDistinct: Boolean)