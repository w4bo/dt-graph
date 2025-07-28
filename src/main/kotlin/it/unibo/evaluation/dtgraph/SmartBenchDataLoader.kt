package it.unibo.evaluation.dtgraph

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import it.unibo.graph.asterixdb.AsterixDBTS
import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraph
import it.unibo.graph.inmemory.MemoryGraphACID
import it.unibo.graph.interfaces.*
import it.unibo.graph.utils.LIMIT
import it.unibo.graph.utils.propTypeFromValue
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import java.io.File
import java.net.URI
import java.nio.file.Paths
import java.util.*
import java.util.concurrent.Executors
import kotlin.system.measureTimeMillis

class SmartBenchDataLoader(
    private val graph: Graph = MemoryGraphACID(),
) {
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
            if (it.isLowerCase()) it.titlecase(
                Locale.getDefault()
            ) else it.toString()
        }}") }

    fun loadData(dataPath: List<String>): Boolean {
        val executor = Executors.newFixedThreadPool(LIMIT).asCoroutineDispatcher()
        val tsList : MutableMap<TS, String> = mutableMapOf()
            val totalTime = measureTimeMillis {
                for (file in dataPath) {
                    println("Loading data from $file")
                    val resource = this::class.java.classLoader.getResource(file)
                    requireNotNull(resource) { "Resource not found: $file" }

                    var count = 0

                    val fileTime = measureTimeMillis {
                        for (obj in loadJsonObjects(resource.toURI(), mapper)) {
                            count++

                            val typeVal = (obj["type"] as? String)
                                ?: error("Missing 'type' in JSON object")
                            val entityId = obj["id"] as? String
                                ?: error("Missing 'id' in JSON object")
                            val nodeLabel = labelFromString(typeVal.replaceFirstChar {
                                if (it.isLowerCase()) it.titlecase(
                                    Locale.getDefault()
                                ) else it.toString()
                            })

                            if(!graphIdList.contains(entityId)) {
                                    // Add static node
                                    val node = graph.addNode(nodeLabel)
                                    val nodeId = node.id
                                    graphIdList[entityId] = nodeId
                                    // Parse its properties
                                    for ((key, value) in obj) {
                                        //if (key === "type" || key === "id") continue

                                        when (value) {
                                            is Map<*, *> -> {
                                                // if property value contains a JSON with a key "id", it's an edge
                                                val destId = value["id"] as? String
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
                                                // elif it's a list, parse each value
                                                for (subVal in value) {
                                                    processProperty(subVal!!, key, nodeId)
                                                }
                                            }

                                            else -> {
                                                // else, it's a static property
                                                processProperty(value, key, nodeId)
                                            }
                                        }
                                    }
                                    // If it's a sensor, check if it has a TS attached
                                    if (typeVal == Labels.Sensor.toString() || typeVal == Labels.VirtualSensor.toString()) {
                                        val sensorId = obj["id"] as String

                                        val labelString = if (labelFromString(typeVal) === Labels.Sensor) "Temperature" else "Presence"
                                        val sensorTsFilePath =
                                            "${Paths.get(file).parent?.toString()}\\timeseries\\${sensorId}.json"
                                        print(sensorTsFilePath)
                                        val sensorTSFile = this::class.java.classLoader.getResource(
                                            sensorTsFilePath
                                        )
                                        sensorTSFile?.let {
                                            println("Found $sensorTsFilePath")
                                            val newTs = tsm.addTS()
                                            val newNode = graph.addNode(labelFromString(labelString), value = newTs.getTSId())
                                            graph.addEdge(hasLabel(labelString), nodeId, newNode.id)
                                            tsList[newTs] = sensorTsFilePath
                                        }

                                    }

//                                }
                            }




                        }
                    }
                    println("Processed $count entities from $file in $fileTime ms")
                }
                println("Starting TS data loading...")
                runBlocking {
                    val jobs = tsList.map{
                            row -> launch(executor) {
                                (row.key as AsterixDBTS).loadInitialData(row.value)
                            }
                    }
                    jobs.joinAll()
                }
                val latentTime = measureTimeMillis {
                    for ((label, source, destId) in leftoverEdgesList) {
                        val target = graphIdList[destId] ?: continue
                        graph.addEdge(label, source, target)
                    }
                }
                println("Processed latent edges in $latentTime ms")

            }
        println("TOTAL ingestion time: $totalTime ms")
        return true
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
