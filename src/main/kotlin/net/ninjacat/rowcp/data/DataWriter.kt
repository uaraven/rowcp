package net.ninjacat.rowcp.data

interface DataWriter {
    fun writeData(startingNode: DataNode, append: Boolean = false)
}
