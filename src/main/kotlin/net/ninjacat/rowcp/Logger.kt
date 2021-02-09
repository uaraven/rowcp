package net.ninjacat.rowcp

import org.fusesource.jansi.Ansi
import org.fusesource.jansi.Ansi.ansi

var currentLogLevel = 1

val V_NORMAL = 1
val V_VERBOSE = 2
val V_SQL = 3

fun logError(e: Exception, message: String?) {
    if (message != null) {
        System.err.println(ansi().fgRed().a(message).reset())
    } else {
        e.printStackTrace()
    }
}

fun log(level: Int, message: String?, noLineFeed: Boolean = false) {
    if (level <= currentLogLevel && message != null) {
        print(ansi().render(message).reset())
        if (!noLineFeed) {
            println()
        }
    }
}

fun log(level: Int, message: Ansi?) {
    if (level <= currentLogLevel && message != null) {
        println(message.reset())
    }
}