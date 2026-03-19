package it.unibo.tests.ci.smartbench

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.core.JsonToken
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import it.unibo.graph.asterixdb.AsterixDBTS
import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.asterixdb.dateToTimestamp
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.TS
import it.unibo.graph.utils.*
import it.unibo.stats.Loader
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
import java.util.logging.Logger
import kotlin.system.measureTimeMillis

class SmartBenchDataLoader(private val graph: Graph, val threads: Int, val dataPath: List<String>): Loader {
    val logger = Logger.getLogger(this.javaClass.name)
    private val mapper: ObjectMapper = jacksonObjectMapper()
    private val tsm = graph.getTSM() as AsterixDBTSM
    private var gsTime: Long = 0
    private var tsTime: Long = 0

    // JSON-id -> graph node-id
    private val graphIdList: MutableMap<String, Long> = mutableMapOf()
    // Edges failed to create because some entity did not exist
    private val leftoverEdgesList: MutableList<Triple<String, Long, String>> = mutableListOf()
    // Label caching
    private val edgeLabelCache: MutableMap<String, String> = mutableMapOf()

    private fun hasLabel(key: String): String = edgeLabelCache.getOrPut(key) { "has${key.replaceFirstChar { if (it.isLowerCase()) it.titlecase(Locale.getDefault()) else it.toString() }}" }

    override fun loadData() {
        val executor = Executors.newFixedThreadPool(threads).asCoroutineDispatcher()
        val tsList: MutableMap<TS, String> = mutableMapOf()


        for (file in dataPath) {
            logger.info { "Loading $file..." }
            val path = Paths.get(file).toAbsolutePath().normalize()
            require(Files.exists(path)) { "File not found: $file" }
            for (obj in loadJsonObjects(path.toUri(), mapper)) {
                val typeVal = (obj[TYPE] as? String) ?: error("Missing \"${TYPE}\" in JSON object")
                val entityId = obj[ID] as? String ?: error("Missing \"${ID}\" in JSON object")
                val nodeLabel = typeVal.replaceFirstChar { if (it.isLowerCase()) it.titlecase(Locale.getDefault()) else it.toString() }

                gsTime += measureTimeMillis {
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

                                else -> processProperty(value, key, nodeId)
                            }
                        }

                        // If it's a sensor, check if it has a TS attached
                        if (typeVal == Sensor || typeVal == VirtualSensor) {
                            val sensorId = obj["id"] as String
                            val labelString = if (typeVal == Sensor) "Temperature" else "Presence"
                            val sensorTsFilePath = path.parent
                                ?.resolve("timeseries")
                                ?.resolve("$sensorId.json")
                                ?.toAbsolutePath()
                                ?.normalize()
                            if (sensorTsFilePath != null && Files.exists(sensorTsFilePath)) {
                                val newNode = graph.addNode(labelString, isTs = true)
                                val newTs = tsm.addTS(newNode.id)
                                graph.addEdge(hasLabel(labelString), nodeId, newNode.id)
                                tsList[newTs] = sensorTsFilePath.toString()
                            }
                        }
                    }
                }
            }
        }
        gsTime += measureTimeMillis {
            for ((label, source, destId) in leftoverEdgesList) {
                val target = graphIdList[destId] ?: continue
                graph.addEdge(label, source, target)
            }
        }
        runBlocking {
            val jobs = tsList.map { row ->
                launch(executor) { loadTS(row.key as AsterixDBTS, row.value) }
            }
            jobs.joinAll()
        }
        tsm.asterixHTTPClient.stopFeed()
        tsm.asterixHTTPClient.dropFeed()
        tsm.asterixHTTPClient.createSpatialIndex()
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
            Files.newBufferedReader(filePath).useLines { lines ->
                for (line in lines) {
                    if (line.isNotBlank()) {
                        val json: JsonNode = mapper.readTree(line)
                        val label = if (json.get(TYPE).textValue() == "Temperature") "temperature" else "presence"
                        tsTime += measureTimeMillis {
                            ts.add(
                                label = json.get(TYPE).textValue(),
                                timestamp = dateToTimestamp(json.get("timestamp").textValue()),
                                location = json.get(LOCATION).textValue(),
                                value = json.get("payload").get(label).longValue(),
                                isUpdate = false
                            )
                        }
                    }
                }
            }
        } else {
            throw UnsupportedOperationException("File not found: $filePath")
        }
    }

    private fun loadJsonObjects(file: URI, mapper: ObjectMapper): Sequence<Map<String, Any>> {
        val factory = JsonFactory()
        val parser = factory.createParser(File(file))
        parser.codec = mapper

        if (parser.nextToken() != JsonToken.START_ARRAY) {
            throw IllegalStateException("Expected JSON array")
        }

        return sequence {
            while (parser.nextToken() == JsonToken.START_OBJECT) {
                @Suppress("UNCHECKED_CAST")
                val obj: Map<String, Any> = parser.readValueAs(Map::class.java) as Map<String, Any>
                yield(obj)
            }
        }
    }

    override fun getGSTime(): Long = gsTime

    override fun getTSTime(): Long = tsTime
}