package net.ninjacat.dtc.data

import java.net.URI

object utils {

    fun initializeDatabase(jdbcUrl: String) {
        if (!jdbcUrl.startsWith("jdbc:")) {
            throw RuntimeException("Invalid JDBC connection string: @|yellow $jdbcUrl")
        }
        val driver = when (val scheme = URI.create(jdbcUrl.substring(5)).scheme) {
            "mysql", "mariadb" -> "org.mariadb.jdbc.Driver"
            "postgresql" -> "org.postgresql.Driver"
            "h2" -> "org.h2.Driver"
            else -> throw RuntimeException("JDBC scheme $scheme is not supported")
        }
        Class.forName(driver)
    }

}