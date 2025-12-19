package it.unibo.ingestion

import com.fasterxml.jackson.core.JsonFactory
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import it.unibo.graph.asterixdb.AsterixDBTS
import it.unibo.graph.asterixdb.AsterixDBTSM
import it.unibo.graph.inmemory.MemoryGraphACID
import it.unibo.graph.interfaces.*
import it.unibo.graph.query.Filter
import it.unibo.graph.query.Operators
import it.unibo.graph.query.Step
import it.unibo.graph.query.search
import it.unibo.graph.utils.LIMIT
import it.unibo.graph.utils.loadProps
import it.unibo.graph.utils.propTypeFromValue
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

val props = loadProps()

val TYPE_ATTRIBUTE = props["node_label_attribute"]?.toString() ?: "type"
val ID_ATTRIBUTE = props["node_id_attribute"]?.toString() ?: "id"
val SENSORS = props["sensors_labels"]?.toString()?.split(",") ?: listOf("Sensor")
val TIMESERIES_TYPE_ATTR = props["timeseries_type_attribute"]?.toString()

class SearchDataLoader(
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

    fun loadData(dataPath: List<String>, threads: Int = LIMIT): Pair<Long, Long> {
        val executor = Executors.newFixedThreadPool(threads).asCoroutineDispatcher()
        val tsList : MutableMap<TS, String> = mutableMapOf()
        val graphLoadingTime = measureTimeMillis {
            for (file in dataPath) {
                println("Loading data from $file")
                val path = Paths.get(file).toAbsolutePath().normalize()
                require(Files.exists(path)) { "File not found: $file" }

                var count = 0

                val fileTime = measureTimeMillis {
                    for (obj in loadJsonObjects(path.toUri(), mapper)) {
                        count++

                        val typeVal = (obj[TYPE_ATTRIBUTE] as? String)
                            ?: error("Missing 'type' in JSON object")
                        val entityId = obj[ID_ATTRIBUTE] as? String
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
                                        val destId = value[ID_ATTRIBUTE] as? String
                                        if (destId != null) {
                                            val edgeLabel = hasLabel(key)
                                            val destNodes = search(graph, listOf(Step(null, listOf(Filter(ID_ATTRIBUTE, Operators.EQ, destId)))))
                                            // If destination node is not present yet, store edge for later process
                                            if(destNodes.size == 0){
                                                leftoverEdgesList += Triple(edgeLabel, nodeId, destId)
                                            }else{
                                                // Else, build the edge
                                                destNodes.forEach { destnode -> destnode.result.forEach{
                                                    graph.addEdge(edgeLabel, nodeId, it.id.toLong())
                                                } }
                                            }

                                            val targetId = graphIdList[destId]
                                            if (targetId != null) {
                                                graph.addEdge(edgeLabel, nodeId, targetId)
                                            } else {

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
                            if (SENSORS.contains(typeVal)) {
                                val sensorId = obj[ID_ATTRIBUTE] as String
                                val labelString = obj[TIMESERIES_TYPE_ATTR] as String
                                val sensorTsFilePath = path.parent
                                    ?.resolve("timeseries")
                                    ?.resolve("$sensorId.json")
                                    ?.toAbsolutePath()
                                    ?.normalize()

                                if (sensorTsFilePath != null && Files.exists(sensorTsFilePath)) {
                                    val newTs = tsm.addTS()
                                    val newNode = graph.addNode(labelFromString(labelString), value = newTs.getTSId())
                                    graph.addEdge(hasLabel(labelString), nodeId, newNode.id)

                                    tsList[newTs] = sensorTsFilePath.toString()
                                }
                            }

//                                }
                        }




                    }
                }
                println("Processed $count entities from $file in $fileTime ms")
            }

        }
            println("Starting TS data loading...")
            val tsLoadingTime = measureTimeMillis {
                if(threads == 1){
                    tsList.forEach{ row -> (row.key as AsterixDBTS).loadInitialData(row.value
                    )}
                }else{
                    runBlocking {
                        val jobs = tsList.map{
                                row -> launch(executor) {
                            (row.key as AsterixDBTS).loadInitialData(row.value)
                        }
                        }
                        jobs.joinAll()
                    }
                }
            }
            println("IT TOOK $tsLoadingTime ms to load TS data")
            val latentTime = measureTimeMillis {
                for ((label, source, destId) in leftoverEdgesList) {
                    val target = graphIdList[destId] ?: continue
                    graph.addEdge(label, source, target)
                }
            }
            println("Processed latent edges in $latentTime ms")

        println("TOTAL ingestion time: ${graphLoadingTime+tsLoadingTime} ms")
        return Pair(graphLoadingTime, tsLoadingTime)
    }

    private fun processProperty(property: Any, key: String, nodeId: Long) {
        if (property is Map<*, *> && property[ID_ATTRIBUTE] is String) {
            val destId = property[ID_ATTRIBUTE] as String
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
