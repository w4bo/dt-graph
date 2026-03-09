package it.unibo.ingestion

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import it.unibo.graph.asterixdb.AsterixDBTS
import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.asterixdb.dateToTimestamp
import it.unibo.graph.inmemory.MemoryGraphACID
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.Label
import it.unibo.graph.interfaces.TS
import it.unibo.graph.interfaces.labelFromString
import it.unibo.graph.utils.*
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.io.File
import java.net.URI
import java.nio.file.Files
import java.nio.file.Paths
import java.util.*
import java.util.concurrent.Executors
import kotlin.system.measureTimeMillis

class SmartBenchDataLoader(private val graph: Graph = MemoryGraphACID()) {
    private val mapper: ObjectMapper = jacksonObjectMapper()
    private val tsm = graph.getTSM() as AsterixDBTSM

    // JSON-id -> graph node-id
    private val graphIdList: MutableMap<String, Long> = mutableMapOf()

    // Edges failed to create because some entity did not exist
    private val leftoverEdgesList: MutableList<Triple<Label, Long, String>> = mutableListOf()

    // Label caching
    private val edgeLabelCache: MutableMap<String, Label> = mutableMapOf()

    private fun hasLabel(key: String): Label =
        edgeLabelCache.getOrPut(key) { labelFromString("has${key.replaceFirstChar {
            if (it.isLowerCase()) it.titlecase(Locale.getDefault()) else it.toString()
        }}") }

    fun loadData(dataPath: List<String>, threads: Int = LIMIT): Pair<Long, Long> {
        val executor = Executors.newFixedThreadPool(threads).asCoroutineDispatcher()
        val tsList: MutableMap<TS, String> = mutableMapOf()

        val graphLoadingTime = measureTimeMillis {
            for (file in dataPath) {
                val path = Paths.get(file).toAbsolutePath().normalize()
                require(Files.exists(path)) { "File not found: $file" }
                var count = 0
                val fileTime = measureTimeMillis {
                    for (obj in loadJsonObjects(path.toUri(), mapper)) {
                        count++

                        val typeVal = (obj[TYPE] as? String)?: error("Missing \"$TYPE\" in JSON object")
                        val entityId = obj[ID] as? String?: error("Missing \"$ID\" in JSON object")
                        val nodeLabel = labelFromString(
                            typeVal.replaceFirstChar {
                                if (it.isLowerCase()) it.titlecase(Locale.getDefault()) else it.toString()
                            }
                        )

                        if (!graphIdList.contains(entityId)) {
                            // Add static node
                            val node = graph.addNode(nodeLabel)
                            val nodeId = node.id
                            graphIdList[entityId] = nodeId

                            // Parse its properties
                            for ((key, value) in obj) {
                                when (value) {
                                    is Map<*, *> -> {
                                        val destId = value[ID] as? String
                                        if (destId != null) {
                                            val edgeLabel = hasLabel(key)
                                            val targetId = graphIdList[destId]
                                            if (targetId != null) {
                                                graph.addEdge(edgeLabel, nodeId, targetId)
                                            } else {
                                                leftoverEdgesList += Triple(edgeLabel, nodeId, destId)
                                            }
                                        }
                                    }

                                    is List<*> -> {
                                        for (subVal in value) {
                                            processProperty(subVal!!, key, nodeId)
                                        }
                                    }

                                    else -> {
                                        processProperty(value, key, nodeId)
                                    }
                                }
                            }

                            // If it's a sensor, check if it has a TS attached
                            if (typeVal == Label.Sensor.toString() || typeVal == Label.VirtualSensor.toString()) {
                                val sensorId = obj["id"] as String
                                val labelString = if (labelFromString(typeVal) === Label.Sensor) "Temperature" else "Presence"

                                val sensorTsFilePath = path.parent
                                    ?.resolve("timeseries")
                                    ?.resolve("$sensorId.json")
                                    ?.toAbsolutePath()
                                    ?.normalize()

                                if (sensorTsFilePath != null && Files.exists(sensorTsFilePath)) {
                                    val newNode = graph.addNode(labelFromString(labelString), isTs = true)
                                    val newTs = tsm.addTS(newNode.id)
                                    graph.addEdge(hasLabel(labelString), nodeId, newNode.id)
                                    tsList[newTs] = sensorTsFilePath.toString()
                                }
                            }
                        }
                    }
                }
                println("Processed $count entities from $file in $fileTime ms")
            }

            val latentTime = measureTimeMillis {
                for ((label, source, destId) in leftoverEdgesList) {
                    val target = graphIdList[destId] ?: continue
                    graph.addEdge(label, source, target)
                }
            }
            println("Processed latent edges in $latentTime ms")
        }

        println("Starting TS data loading...")
        val tsLoadingTime = measureTimeMillis {
            if (threads > 1) {
                runBlocking {
                    val jobs = tsList.map { row ->
                        launch(executor) { loadTS(row.key as AsterixDBTS, row.value) }
                    }
                    jobs.joinAll()
                }
            } else {
                tsList.forEach { ts -> loadTS(ts.key as AsterixDBTS, ts.value) }
            }
        }
        println("IT TOOK $tsLoadingTime ms to load TS data")
        println("TOTAL ingestion time: ${graphLoadingTime + tsLoadingTime} ms")
        print("Graph contains ${graph.getNodes().size} nodes and  ${graph.getEdges().size} edges")
        return Pair(graphLoadingTime, tsLoadingTime)
    }

    private fun processProperty(property: Any, key: String, nodeId: Long) {
        if (property is Map<*, *> && property["id"] is String) {
            val destId = property["id"] as String
            val edgeLabel = hasLabel(key)
            val targetId = graphIdList[destId]
            if (targetId != null) {
                graph.addEdge(edgeLabel, nodeId, targetId)
            } else {
                leftoverEdgesList += Triple(edgeLabel, nodeId, destId)
            }
        } else {
            graph.addProperty(nodeId, key, property, propTypeFromValue(property))
        }
    }

    fun loadTS(ts: AsterixDBTS, path: String) {
        val projectRoot = Paths.get("").toAbsolutePath().normalize()
        val filePath = projectRoot.resolve(path).normalize()
        if (Files.exists(filePath)) {
            ts.openDataFeedConnection()
            Files.newBufferedReader(filePath).useLines { lines ->
                for (line in lines) {
                    if (line.isNotBlank()) {
                        val json: JsonNode = mapper.readTree(line)
                        val label = if (json.get(TYPE).textValue() == "Temperature") "temperature" else "presence"
                        ts.add(
                            label = labelFromString(json.get(TYPE).textValue()),
                            timestamp = dateToTimestamp(json.get("timestamp").textValue()),
                            location = json.get(LOCATION).textValue(),
                            value = json.get("payload").get(label).longValue(), // TODO: attualmente è fissato sulla sintassi SmartBench
                            isUpdate = false
                        )
                    }
                }
            }
            ts.closeDataFeedConnection(closeRemote = true)
        } else {
            throw UnsupportedOperationException("File not found: $filePath")
        }
    }

    private fun loadJsonObjects(file: URI, mapper: ObjectMapper): Sequence<Map<String, Any>> {
        val factory = JsonFactory()
        val parser = factory.createParser(File(file))
        parser.codec = mapper

        if (parser.nextToken() != com.fasterxml.jackson.core.JsonToken.START_ARRAY) {
            throw IllegalStateException("Expected JSON array")
        }

        return sequence {
            while (parser.nextToken() == com.fasterxml.jackson.core.JsonToken.START_OBJECT) {
                @Suppress("UNCHECKED_CAST")
                val obj: Map<String, Any> = parser.readValueAs(Map::class.java) as Map<String, Any>
                yield(obj)
            }
        }
    }
}
