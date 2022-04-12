/*
 *    Copyright 2022 Oleksiy Voronin
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

package net.ninjacat.rowcp.data

import kotlinx.serialization.decodeFromString
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import net.ninjacat.rowcp.V_NORMAL
import net.ninjacat.rowcp.log
import java.net.URLEncoder
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths

object DbSchemaCache {
    private val cacheStoreDirectory: Path = Paths.get(System.getProperty("user.home")).resolve(".rowcp")

    fun clearCache(jdbcUrl: String) {
        Files.createDirectories(cacheStoreDirectory)
        cacheStoreDirectory.resolve(URLEncoder.encode(jdbcUrl, StandardCharsets.UTF_8)).toFile().delete()
    }

    fun useCache(jdbcUrl: String, schemaGenerator: (SchemaGraph?) -> DbSchema): DbSchema {
        Files.createDirectories(cacheStoreDirectory)
        val cacheFile = cacheStoreDirectory.resolve(URLEncoder.encode(jdbcUrl, StandardCharsets.UTF_8))
        return if (cacheFile.toFile().exists()) {
            log(V_NORMAL, "Reading schema from cache")
            val serializedSchema = Files.readString(cacheFile, StandardCharsets.UTF_8)
            val schemaGraph: SchemaGraph = Json.decodeFromString(serializedSchema)
            log(V_NORMAL, "Tables in schema: @|cyan ${schemaGraph.tables.size} |@")
            schemaGenerator(schemaGraph)
        } else {
            val schema = schemaGenerator(null)
            val serializedSchema = Json.encodeToString(schema.getSchemaGraph())
            Files.writeString(cacheFile, serializedSchema, StandardCharsets.UTF_8)
            schema
        }
    }
}
