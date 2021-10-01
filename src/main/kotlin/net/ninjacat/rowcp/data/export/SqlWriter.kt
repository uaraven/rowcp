/*
 *    Copyright 2021 Oleksiy Voronin
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package net.ninjacat.rowcp.data.export

import net.ninjacat.rowcp.V_NORMAL
import net.ninjacat.rowcp.V_VERBOSE
import net.ninjacat.rowcp.data.DataNode
import net.ninjacat.rowcp.data.DataWriter
import net.ninjacat.rowcp.log
import java.io.File
import java.io.FileOutputStream
import java.io.PrintStream

class SqlWriter(targetUrl: String) : DataWriter {

    private val fileName = targetUrl.substring(5)

    override fun writeData(startingNode: DataNode) {
        val updates = prepareUpdates(startingNode)

        val printer =
            if (fileName == "--" || fileName == "stdout") {
                System.out
            } else {
                log(V_NORMAL, "Writing SQL statements into @|blue ${fileName}|@")
                PrintStream(FileOutputStream(File(fileName)))
            }

        updates.forEach { printer.println(it) }
        printer.flush()
    }

    fun prepareUpdates(startingNode: DataNode): List<String> {
        val beforeUpdates = startingNode.before.flatMap { prepareUpdates(it) }
        val afterUpdates = startingNode.after.flatMap { prepareUpdates(it) }

        log(V_VERBOSE, "Preprocessing rows for ${startingNode.tableName}")
        val updates = startingNode.rows.map { row ->
            val columnNames = row.columns.map { it.columnName }
            val columnValue = row.columns.map { it.asSqlText() }
            "INSERT INTO ${row.table.name}(${columnNames.joinToString(", ")})\n" +
                    "VALUES(${columnValue.joinToString(", ")})"
        }

        return beforeUpdates + updates + afterUpdates
    }
}


