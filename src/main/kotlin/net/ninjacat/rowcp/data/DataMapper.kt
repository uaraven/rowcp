package net.ninjacat.rowcp.data

import net.ninjacat.rowcp.Args

class ValidationException(message: String) : RuntimeException(message)

class DataMapper(val args: Args, val targetSchema: DbSchema) : Mapper {

    override fun mapToTarget(source: DataNode): DataNode {
        val before = source.before.map { mapToTarget(it) }
        val after = source.after.map { mapToTarget(it) }

        val rows = source.rows.map { mapRow(it) }

        return DataNode(source.tableName, rows, before, after)
    }

    private fun mapRow(row: DataRow): DataRow {
        val validationErrors = mutableListOf<String>()

        val targetTable = targetSchema.getSchemaGraph().table(row.tableName())
        val mappedColumns: List<ColumnData>
        var nullableTargetColumns: List<ColumnData>
        if (targetTable == null) {
            validationErrors.add("Table '${row.tableName()}' does not exist in the target database")
            mappedColumns = row.columns
            nullableTargetColumns = listOf()
        } else {
            mappedColumns = row.columns.filter { targetTable.columnNames.contains(it.columnName) }
            val unmappedColumns = row.columns.filterNot { targetTable.columnNames.contains(it.columnName) }

            // collect nullable columns in target schema that don't exist in the source and populate them with nulls
            nullableTargetColumns = targetTable.columns.filter { !row.columnNames.contains(it.name) && it.nullable }
                .map { ColumnData(it.name, it.type, null) }

            val unfulfilledTargetColumns =
                targetTable.columns.filter { !row.columnNames.contains(it.name) && !it.nullable }

            if (unmappedColumns.isNotEmpty() && !args.skipMissingColumns) {
                validationErrors.add("${if (unmappedColumns.size > 1) "Following columns do" else "Column does"} not exist in table '${row.tableName()}' in the target database: $unmappedColumns")
            }
            if (unfulfilledTargetColumns.isNotEmpty()) {
                validationErrors.add("${if (unfulfilledTargetColumns.size > 1) "Following columns do" else "Column does"} not exist in table '${row.tableName()}' in the source database: $unfulfilledTargetColumns")
            }
        }
        if (validationErrors.isNotEmpty()) {
            throw ValidationException(validationErrors.joinToString("\n"))
        }

        return DataRow(row.table, mappedColumns + nullableTargetColumns)
    }
}