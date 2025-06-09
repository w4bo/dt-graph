package it.unibo.evaluation.dtgraph

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.inmemory.MemoryGraph
import it.unibo.graph.interfaces.Label
import it.unibo.graph.asterixdb.dateToTimestamp
import it.unibo.graph.interfaces.TS
import it.unibo.graph.interfaces.labelFromString
import it.unibo.graph.utils.propTypeFromValue
import java.io.File
import java.net.URI
import kotlin.system.measureTimeMillis

class SmartBenchDataLoader(
    private val graph: Graph = MemoryGraph(),
    tsLabelsList: List<String> = listOf("Measurement", "Observation", "Occupancy", "Presence"),
    virtualSensorTsLabelsList: List<String> = listOf("Occupancy", "Presence")
) {
    private val tsLabels: Set<String> = tsLabelsList.toSet()
    private val virtualSensorTsLabels: Set<String> = virtualSensorTsLabelsList.toSet()

    private val mapper: ObjectMapper = jacksonObjectMapper()
    private val tsm = graph.getTSM()

    // JSON-id -> graph node-id
    private val graphIdList: MutableMap<String, Long> = mutableMapOf()

    // sensorId -> ( PropertyName -> TS )
    private val tSList: MutableMap<String, MutableMap<String, TS>> = mutableMapOf()

    // Edges failed to create because some entity did not exist
    private val leftoverEdgesList: MutableList<Triple<Label, Long, String>> = mutableListOf()

    // Label caching
    private val edgeLabelCache: MutableMap<String, Label> = mutableMapOf()

    private fun hasLabel(key: String): Label =
        edgeLabelCache.getOrPut(key) { labelFromString("has$key") }

    fun loadData(dataPath: List<String>): Boolean {
            val totalTime = measureTimeMillis {
                for (file in dataPath) {
                    println("Loading data from $file")
                    val resource = this::class.java.classLoader.getResource(file)
                    requireNotNull(resource) { "Resource not found: $file" }

                    var count = 0
                    val sb = StringBuilder()

                    val fileTime = measureTimeMillis {
                        for (obj in loadJsonObjects(resource.toURI(), mapper)) {
                            count++

                            val typeVal = obj["type"] as? String
                                ?: error("Missing 'type' in JSON object")
                            val entityId = obj["id"] as? String
                                ?: error("Missing 'id' in JSON object")
                            val nodeLabel = labelFromString(typeVal)

                            // If it's a sensor
                            if (tsLabels.contains(typeVal)) {
                                val sensorId = if (virtualSensorTsLabels.contains(typeVal)) {
                                    (obj["virtualSensor"] as Map<*, *>)["id"] as String
                                } else {
                                    (obj["sensor"] as Map<*, *>)["id"] as String
                                }

                                // Get its TS list
                                val sensorTSMap = tSList[sensorId]

                                if (sensorTSMap != null) {
                                    val labelString = typeVal

                                    // Check if a TS for this sensor and this property exists
                                    val ts: TS = sensorTSMap[labelString]
                                        ?: run {
                                            // If not, create it
                                            val graphSensorId = graphIdList[sensorId]
                                                ?: error("Sensor node missing for $sensorId")
                                            val newTs = tsm.addTS()
                                            val newNode = graph.addNode(nodeLabel, value = newTs.getTSId())
                                            graph.addEdge(hasLabel(labelString), graphSensorId, newNode.id)
                                            sensorTSMap[labelString] = newTs
                                            newTs
                                        }

                                    // Parse measurement
                                    val payloadMap = obj["payload"] as Map<*, *>
                                    val entry = payloadMap.entries.iterator().next()
                                    val measureKey = entry.key as String
                                    val measureLabel = labelFromString(measureKey)
                                    val measureValue = (entry.value as Number).toLong()
                                    val tsTimestamp = dateToTimestamp(obj["timestamp"] as String)

                                    // Parse location
                                    val locationJson: String? = (obj["location"] as? Map<*, *>)?.let { loc ->
                                        sb.clear()
                                        sb.append("{\"type\":\"Point\",\"coordinates\":[")
                                        val itCoords = loc.values.iterator()
                                        sb.append(itCoords.next().toString())
                                        sb.append(',')
                                        sb.append(itCoords.next().toString())
                                        sb.append("]}")
                                        sb.toString()
                                    }

                                    // Add measurement to TS
                                    if (locationJson != null) {
                                        ts.add(
                                            label = measureLabel,
                                            timestamp = tsTimestamp,
                                            value = measureValue,
                                            location = locationJson,
                                            isUpdate = false
                                        )
                                    } else {
                                        ts.add(
                                            label = measureLabel,
                                            timestamp = tsTimestamp,
                                            value = measureValue,
                                            isUpdate = false
                                        )
                                    }
                                }
                            }else{
                                // Otherwise, it's a static node.
                                val node = graph.addNode(nodeLabel)
                                val nodeId = node.id
                                graphIdList[entityId] = nodeId

                                // Parse its properties
                                for ((key, value) in obj) {
                                    if (key === "type" || key === "id") continue

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

                                // 5) Se è nodo sensore, inizializzo mappa TS vuota
                                if (typeVal == "Sensor" || typeVal == "VirtualSensor") {
                                    tSList[entityId] = mutableMapOf()
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
