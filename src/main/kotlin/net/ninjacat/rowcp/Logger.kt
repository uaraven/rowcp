package net.ninjacat.rowcp

import org.fusesource.jansi.Ansi
import org.fusesource.jansi.Ansi.ansi

var currentLogLevel = 1

val V_NORMAL = 1
val V_VERBOSE = 2
val V_SQL = 3

fun logError(message: String?) {
    if (message != null) {
        System.err.println(ansi().fgRed().a(message).reset())
    }
}

fun log(level: Int, message: String?) {
    if (level <= currentLogLevel && message != null) {
        println(ansi().render(message).reset())
    }
}

fun log(level: Int, message: Ansi?) {
    if (level <= currentLogLevel && message != null) {
        println(message.reset())
    }
}