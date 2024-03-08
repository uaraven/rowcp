package net.ninjacat.rowcp.data

import java.net.URI

object Utils {

    fun initializeDatabase(jdbcUrl: String) {
        if (jdbcUrl.startsWith("file:")) {
            return
        }
        if (!isJdbcUrl(jdbcUrl)) {
            throw RuntimeException("Invalid JDBC connection string: @|yellow $jdbcUrl")
        }
        val driver = when (val scheme = getJdbcScheme(jdbcUrl)) {
            "mariadb" -> "org.mariadb.jdbc.Driver"
            "mysql" -> "com.mysql.cj.jdbc.Driver"
            "postgresql" -> "org.postgresql.Driver"
            "jdbc-secretsmanager" -> "com.amazonaws.secretsmanager.sql.AWSSecretsManagerMariaDBDriver"
            "h2" -> "org.h2.Driver"
            else -> throw RuntimeException("JDBC scheme $scheme is not supported")
        }
        Class.forName(driver)
    }

    private fun getJdbcScheme(jdbcUrl: String): String? = if (jdbcUrl.startsWith("jdbc:")) {
        URI.create(jdbcUrl.substring(5)).scheme
    } else {
        jdbcUrl.substring(0, jdbcUrl.indexOf(':'))
    }

    fun getJdbcDriver(jdbcUrl: String): String {
        if (!isJdbcUrl(jdbcUrl)) {
            throw RuntimeException("Invalid JDBC connection string: @|yellow $jdbcUrl")
        }
        return getJdbcScheme(jdbcUrl)!!
    }

    private fun isJdbcUrl(jdbcUrl: String) = jdbcUrl.startsWith("jdbc:") or jdbcUrl.startsWith("jdbc-secretsmanager")

    fun <T : AutoCloseable, R> T.use(block: T.() -> R): R {
        try {
            return block(this)
        } finally {
            this.close()
        }
    }

}
