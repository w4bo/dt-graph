package it.unibo.evaluation.dtgraph;

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.ObjectMapper
import it.unibo.graph.interfaces.Graph;
import it.unibo.graph.inmemory.MemoryGraph
import it.unibo.graph.interfaces.*
import com.fasterxml.jackson.module.kotlin.*
import it.unibo.graph.asterixdb.dateToTimestamp
import it.unibo.graph.interfaces.Labels.*
import it.unibo.graph.utils.propTypeFromValue
import java.io.File
import java.net.URI
import kotlin.system.measureTimeMillis

class SmartBenchDataLoader constructor(
      val graph : Graph = MemoryGraph(),
      val tsLabels : List<String> = listOf("Measurement", "Observation", "Occupancy","Presence"),
      val virtualSensorTsLabels: List<String> = listOf("Occupancy","Presence")
) {

    private val mapper: ObjectMapper = jacksonObjectMapper()
    private val tsm = graph.getTSM()

    // Maps JSON ids to Graph ids, needed for creating edges
    private val graphIdList : MutableMap<String, Long> = mutableMapOf()

    // TS IDs list for fast retrieval
    // Map<Pair<DeviceId, GraphNode>, Map<Property,TS_ID>>
    private val tSList : MutableMap<Pair<String,N>, MutableMap<String, TS>> = mutableMapOf()

    // Might have to process an edge before both nodes exists, if first adds fails store them here and add later
    private val latentEdgesList : MutableList<Triple<Label, Long, String>> = mutableListOf()


    fun loadData(dataPath : List<String>): Boolean {
        try {
            val ingestionTime = measureTimeMillis {
                // Foreach file
                for (file in dataPath) {
                    println("Loading data from $file")
                    val resource = this::class.java.classLoader.getResource(file)
                    requireNotNull(resource) { "Resource not found: $file" }
                    var count: Int = 0

                    val fileIngestionTime = measureTimeMillis {
                        // Foreach entity in JSON file
                        for (obj in loadJsonObjects(resource.toURI(), mapper)) {
                            count++
                            val node: N
                            val graphNodeId: Long

                            var nodeLabel: Label = Measurement
                            var entityId = ""
                            try {
                                nodeLabel = labelFromString(obj["type"] as String)
                                entityId = obj["id"] as String
                            } catch (e: Exception) {
                                println(e)
                            }

                            var sensorId = ""
                            // If it's a TS node
                            if (tsLabels.contains(nodeLabel.toString())) {

                                try {
                                    sensorId = if (virtualSensorTsLabels.contains(nodeLabel.toString())) {
                                        (obj["virtualSensor"] as Map<*, *>)["id"] as String
                                    } else {
                                        (obj["sensor"] as Map<*, *>)["id"] as String
                                    }
                                } catch (e: Exception) {
                                    println(e)
                                }

                                // If device node already exists
                                if (tSList.keys.map { it.first }.contains(sensorId)) {
                                    val sensorTS = tSList.entries.find { it.key.first == sensorId }!!.value
                                    val ts: TS

                                    // If this device TS  already exists
                                    if (sensorTS.contains(nodeLabel.toString())) {
                                        ts = sensorTS[nodeLabel.toString()]!!
                                    } else {
                                        // Create TS
                                        val graphSensorId = graphIdList[sensorId]!!
                                        ts = graph.getTSM().addTS()
                                        node = graph.addNode(nodeLabel, value = ts.getTSId())
                                        graphNodeId = node.id
                                        graph.addEdge(labelFromString("has${nodeLabel}"), graphSensorId, node.id)
                                        //Add new TS to tsList
                                        sensorTS[nodeLabel.toString()] = ts
                                    }
                                    try {
                                        val payload = obj["payload"] as Map<*, *>
                                        val label = labelFromString(payload.keys.first() as String)
                                        val timestamp = dateToTimestamp(obj["timestamp"] as String)
                                        val value = (payload.values.first() as Number).toLong()

                                        val location = (obj["location"] as? Map<*, *>)?.let { loc ->
                                            val coords = loc.values.joinToString(" ")
                                            """{ "type": "Point", "coordinates": [$coords]}"""
                                        }
                                        if(location != null){
                                            ts.add(
                                                label = label,
                                                timestamp = timestamp,
                                                value = value,
                                                location = location,
                                                isUpdate = false
                                            )
                                        }else{
                                            ts.add(
                                                label = label,
                                                timestamp = timestamp,
                                                value = value,
                                                isUpdate = false
                                            )
                                        }


                                    } catch (e: Exception) {
                                        println(e)
                                    }

                                } else {
                                    // Also create the sensor
                                    //TODO: Should never happen theoretically
                                    continue
                                }
                            } else {
                                //It's a static node
                                node = graph.addNode(nodeLabel)
                                graphNodeId = node.id

                                graphIdList[entityId] = graphNodeId

                                // Handle Node properties
                                obj
                                    .filterKeys { it != "type" && it != "id" }
                                    .forEach { (key, value) ->
                                        if (value is List<*>) {
                                            for (subValue in value) {
                                                processProperty(subValue!!, key, graphNodeId)
                                            }
                                        } else {
                                            processProperty(value, key, graphNodeId)
                                        }
                                    }
                                // Add sensor to sensors list
                                if (nodeLabel.toString() == "Sensor" || nodeLabel.toString() == "VirtualSensor") {
                                    tSList[Pair(entityId, node)] = mutableMapOf()
                                }

                            }

                        }
                    }
                    println("Processed $count entities from $file")
                    println("Ingestion time for $file: $fileIngestionTime ms")
                }

                // Now process leftover edges
                for ((label, source, dest) in latentEdgesList) {
                    graph.addEdge(label, source, graphIdList[dest]!!)
                }
            }
            println("Overall ingestion time: $ingestionTime ms")

        } catch (e: Exception) {
            throw e
        }

        return true

    }

    private fun processProperty(property:Any, key:String, graphNodeId:Long) {
        //If it's an edge
        if (property is Map<*, *> && "id" in property) {
            val destId = property["id"] as String
            try {
                // Create edge
                graph.addEdge(labelFromString("has${key}"), graphNodeId, graphIdList[destId]!!)
            } catch (e: Exception){
                // If it fails (dest edge does not exist), add it later
                latentEdgesList.add(Triple(labelFromString("has${key}"), graphNodeId, destId))
            }

        } else {
            // It's a property
            graph.addProperty(graphNodeId, key, property, propTypeFromValue(property))
        }
    }
    private fun loadJsonObjects(file: URI, mapper: ObjectMapper): Sequence<Map<String, Any>> {
        val factory = JsonFactory()
        val parser = factory.createParser(File(file))
        parser.codec = mapper

        // Sposta il parser fino all'inizio dell'array
        if (parser.nextToken() != com.fasterxml.jackson.core.JsonToken.START_ARRAY) {
            throw IllegalStateException("Expected JSON array")
        }

        return sequence {
            while (parser.nextToken() == com.fasterxml.jackson.core.JsonToken.START_OBJECT) {
                val obj: Map<String, Any> = parser.readValueAs(Map::class.java) as Map<String, Any>
                yield(obj)
            }
        }
    }
}
