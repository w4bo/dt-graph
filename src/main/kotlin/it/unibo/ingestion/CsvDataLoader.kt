package it.unibo.ingestion

import it.unibo.graph.interfaces.Graph
import java.io.File
import kotlin.io.NoSuchFileException

class CsvLoader(private val delimiter: String = "|", private val graph: Graph) {

    /**
     * Entry point for CSV ingestion.
     * Distinguishes between Node and Edge files to apply specific parsing logic.
     */
    fun processStreaming(file: File, isEdgeFile: Boolean) {
        if (!file.exists()) throw NoSuchFileException(file)

        file.bufferedReader().use { reader ->
            val lines = reader.lineSequence()
            val iterator = lines.iterator()

            if (!iterator.hasNext()) return // Empty file check

            // 1. Extract and map the header: Column Name -> Index
            val header = iterator.next().split(delimiter).map { it.trim() }
            val headerMap = header.withIndex().associate { it.value to it.index }

            // 2. Process remaining lines using the header mapping
            iterator.forEachRemaining { line ->
                val row = line.split(delimiter).map { it.trim() }
                if (row.size == header.size) { // Structural integrity check
                    if (isEdgeFile) {
                        parseEdge(headerMap, row)
                    } else {
                        parseNode(headerMap, row)
                    }
                }
            }
        }
    }

    /**
     * Parses a node row based on Neo4j format (e.g., id:ID, name:STRING).
     */
    private fun parseNode(headerMap: Map<String, Int>, row: List<String>) {
        // Find the ID column (handling potential naming variations like id:ID(TagClass))
        val idKey = headerMap.keys.find { it.contains(":ID", ignoreCase = true) } ?: return
        val id = row[headerMap[idKey]!!]

        // Extract other properties from the row
        val properties = headerMap.filter { it.key != idKey }.mapValues { row[it.value] }

        graph.addNode(id, properties)
    }

    /**
     * Parses an edge row based on Neo4j format (:START_ID and :END_ID).
     */
    private fun parseEdge(headerMap: Map<String, Int>, row: List<String>) {
        val startKey = headerMap.keys.find { it.contains(":START_ID", ignoreCase = true) } ?: return
        val endKey = headerMap.keys.find { it.contains(":END_ID", ignoreCase = true) } ?: return

        val startId = row[headerMap[startKey]!!]
        val endId = row[headerMap[endKey]!!]

        graph.addEdge(startId, endId)
    }
}