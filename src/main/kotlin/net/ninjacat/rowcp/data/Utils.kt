package net.ninjacat.rowcp.data

import java.net.URI

object Utils {

    fun initializeDatabase(jdbcUrl: String) {
        if (jdbcUrl.startsWith("file:")) {
            return
        }
        if (!jdbcUrl.startsWith("jdbc:")) {
            throw RuntimeException("Invalid JDBC connection string: @|yellow $jdbcUrl")
        }
        val driver = when (val scheme = URI.create(jdbcUrl.substring(5)).scheme) {
            "mariadb" -> "org.mariadb.jdbc.Driver"
            "mysql" -> "com.mysql.cj.jdbc.Driver"
            "postgresql" -> "org.postgresql.Driver"
            "h2" -> "org.h2.Driver"
            else -> throw RuntimeException("JDBC scheme $scheme is not supported")
        }
        Class.forName(driver)
    }

    fun getJdbcDriver(jdbcUrl: String): String {
        if (!jdbcUrl.startsWith("jdbc:")) {
            throw RuntimeException("Invalid JDBC connection string: @|yellow $jdbcUrl")
        }
        return URI.create(jdbcUrl.substring(5)).scheme
    }

    fun <T : AutoCloseable, R> T.use(block: T.() -> R): R {
        try {
            return block(this)
        } finally {
            this.close()
        }
    }

}