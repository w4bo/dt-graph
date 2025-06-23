package it.unibo.graph.asterixdb

import it.unibo.graph.interfaces.*
import it.unibo.graph.query.AggOperator
import it.unibo.graph.query.Aggregate
import it.unibo.graph.query.Filter
import it.unibo.graph.structure.CustomVertex
import it.unibo.graph.utils.*
import org.json.JSONObject
import java.io.File
import java.io.OutputStream
import java.io.PrintWriter
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import java.net.Socket
import kotlinx.coroutines.*

class AsterixDBTS(
    override val g: Graph,
    val id: Long,
    clusterControllerHost: String,
    dataFeedIp: String,
    private val dataverse: String,
    datatype: String,
    busyPorts: MutableSet<Int>,
    seed: Int = SEED,
    inputPath: String? = null
) : TS {

    private var dataset: String
    var dataFeedPort: Int

    private var socket: Socket
    private var outputStream: OutputStream
    private var writer: PrintWriter
    private val mapper = ObjectMapper()

    private val asterixHTTPClient: AsterixDBHTTPClient = AsterixDBHTTPClient(
        clusterControllerHost,
        dataFeedIp,
        busyPorts,
        dataverse,
        datatype,
        id,
        seed
    )

    init {
        dataset = asterixHTTPClient.getDataset()
        dataFeedPort = asterixHTTPClient.getDataFeedPort()

        socket = Socket(dataFeedIp, dataFeedPort)
        outputStream = socket.getOutputStream()
        writer = PrintWriter(outputStream, true)

        inputPath?.let{
            val job = GlobalScope.launch(Dispatchers.IO) {
                try{
                    loadInitialData(it)
                }catch(e: Exception){
                    println(e)
                    e.printStackTrace()
                }

            }
        }

    }

    private fun loadInitialData(path: String) {
        val resource = this::class.java.classLoader.getResource(path)
        if (resource != null) {
            resource.openStream().bufferedReader().useLines { lines ->
                for (line in lines) {
                    if (line.isNotBlank()) {
                        val json: JsonNode = mapper.readTree(line)
                        add(
                            label = labelFromString(json.get("type").textValue()),
                            timestamp = dateToTimestamp(json.get("timestamp").textValue()),
                            location = json.get("location").textValue(),
                            //TODO: FIX THIS, E' FISSATO SULLA SINTASSI DI SMARTBENCH
                            value = json.get("payload").get("temperature").longValue(),
                            isUpdate = false

                        )
                    }
                }
            }
        }
    }
    override fun getTSId(): Long = id

    override fun add(n: N, isUpdate: Boolean): N {
        if (isUpdate) {
            updateTs(n)
        } else {
            val measurement = """
            {
                "timestamp": ${n.fromTimestamp},
                "property": "${n.label}",
                ${parseLocationToWKT(n.getProps(name = LOCATION).firstOrNull())}
                ${relationshipToAsterixCitizen(n.relationships)}
                ${propertiesToAsterixCitizen(n.getProps())}
                "fromTimestamp": datetime("${timestampToISO8601(n.fromTimestamp)}"),
                "toTimestamp": datetime("${timestampToISO8601(n.toTimestamp)}"),
                "value": ${n.value}
            }
            """.trimIndent()
            writer.println(measurement)
            if (writer.checkError()) {
                println("Errore durante la scrittura nel file!")
                throw Exception("Coudln't insert data into AsterixDB")
            }
        }
        //TODO: Nei test, cambia apertura chiusura dei socket
        //TODO: Handle negative results
        return n
    }

    private fun groupby(by: List<Aggregate>): Pair<List<String>, List<String>>? {
        //TODO: ATM handles only SUM AggOperator
        return when {
            by.isEmpty() -> null
            else -> {
                val selectClause = by.filter { it.operator != null }
                    .map {
                        val prop = if (it.property == "value") "`value`" else it.property
                        "${AggOperator.SUM.name}($prop) as $prop"
                    }

                val groupbyClause = by.filter { it.operator == null }.mapNotNull { it.property }
                return Pair(selectClause, groupbyClause)
            }
        }
    }

    override fun getValues(by: List<Aggregate>, filters: List<Filter>): List<N> {
        val selectQuery: String
        val groupByClause: List<String>
        val defaultAggregatorsList = listOf(PROPERTY, FROM_TIMESTAMP, TO_TIMESTAMP)
        val defaultGroupByAggregators =
            ", $PROPERTY, COUNT(*) as count, MIN($FROM_TIMESTAMP) as $FROM_TIMESTAMP, MAX($TO_TIMESTAMP) as $TO_TIMESTAMP"

        // Select *each where predicate*
        var whereAggregators = filters
            .filter { !defaultAggregatorsList.contains(it.property) && it.property != VALUE }.joinToString(
                ",",
                prefix = if (filters.any { !defaultGroupByAggregators.contains(it.property) && it.property != VALUE }) "," else ""
            ) { it.property }

        if (by.isEmpty()) {
            // If not group by, add mandatory attributes for graph representation
            whereAggregators += ", `value`, timestamp, properties, relationships"
            selectQuery = """
                    USE $dataverse;
                    SELECT ${defaultAggregatorsList.joinToString(",")} $whereAggregators
                    FROM $dataset
                    ${applyFilters(filters)}
                """.trimIndent()
        } else {
            val groupBy = groupby(by)!!
            val selectClause = groupBy.first
            groupByClause = groupBy.second
            val groupByPredicates = groupByClause.joinToString(",")

            // GROUP BY property + all GROUP BY predicates + WHERE predicates
            val groupByPart =
                if (groupByPredicates.isNotEmpty()) "GROUP BY $groupByPredicates, $PROPERTY $whereAggregators" else "GROUP BY $PROPERTY $whereAggregators"

            selectQuery = """
                USE $dataverse;
                SELECT ${
                groupByClause.joinToString(",").let { if (it.isNotEmpty()) "$it," else "" }
            } ${selectClause.joinToString(",")} 
              $defaultGroupByAggregators 
              $whereAggregators
                FROM $dataset
                ${applyFilters(filters)}
                $groupByPart;
            """.trimIndent()
        }

        when (val result = asterixHTTPClient.selectFromAsterixDB(selectQuery, isGroupBy = by.isNotEmpty())) {

            // If it's a simple select *
            is AsterixDBResult.SelectResult -> {
                if (result.entities.isEmpty) return emptyList()
//                val minTimestampTo = result.entities.map{(it as JSONObject).get("toTimestamp")}
//                val maxTimestampFrom = result.entities.map{(it as JSONObject).get("fromTimestamp")}
                return result.entities.map { selectNodeFromJsonObject((it as JSONObject)) }
            }

            // If we are aggregating
            is AsterixDBResult.GroupByResult -> {
                if (result.entities.isEmpty) return emptyList()
                val aggOperator = by.first { it.operator != null }.property!!.toString()

                val fromTimestamp = dateToTimestamp((0 until result.entities.length()).minOf {
                    result.entities.getJSONObject(
                        it
                    ).getString(FROM_TIMESTAMP)
                })

                val toTimestamp = dateToTimestamp((0 until result.entities.length()).maxOf {
                    result.entities.getJSONObject(
                        it
                    ).getString(TO_TIMESTAMP)
                })
                return result.entities.toList().map { it as HashMap<*, *> }.map {
                    val aggValue = Pair((it[aggOperator] as? Number)?.toDouble(), (it["count"] as? Number)?.toDouble())
                    N.createVirtualN(
                        labelFromString(it[PROPERTY]!!.toString()),
                        aggregatedValue = aggValue,
                        fromTimestamp = fromTimestamp,
                        toTimestamp = toTimestamp,
                        g = g,
                        properties = it.keys.filter { key -> key != aggOperator && !defaultAggregatorsList.contains(key.toString()) }
                            .map { key ->
                                parseProp(
                                    it,
                                    fromTimestamp = fromTimestamp,
                                    toTimestamp = toTimestamp,
                                    key = key.toString(),
                                    g = g
                                )
                            } + (
                                if (aggOperator != VALUE) {
                                    listOf(
                                        P(
                                            DUMMY_ID,
                                            sourceId = DUMMY_ID.toLong(),
                                            key = aggOperator,
                                            value = aggValue,
                                            type = PropType.STRING,
                                            sourceType = NODE,
                                            g = g,
                                            fromTimestamp = fromTimestamp,
                                            toTimestamp = toTimestamp
                                        )
                                    )
                                } else {
                                    emptyList()
                                }
                                )
                    )
                }
            }

            else -> {
                println("Error occurred while performing query \n $selectQuery")
                return emptyList()
            }
        }
    }

    override fun get(timestamp: Long): N {
        val selectQuery = """
        USE $dataverse;
        SELECT * FROM $dataset
        WHERE timestamp = $timestamp
        """.trimIndent()
        when (val result = asterixHTTPClient.selectFromAsterixDB(selectQuery)) {
            is AsterixDBResult.SelectResult -> {
                if (result.entities.length() > 0) {
                    return selectNodeFromJsonObject((result.entities[0] as JSONObject).get(dataset) as JSONObject)

                } else {
                    throw IllegalArgumentException()
                }
            }

            else -> {
                throw IllegalArgumentException("Error occurred while performing query \n $selectQuery")
            }
        }
    }

    fun deleteTs() {
        asterixHTTPClient.deleteTs()
        writer.close()
    }

    private fun updateTs(n: N) {
        val upsertStatement = """
            USE $dataverse;
            UPSERT INTO $dataset ([{
                "timestamp": ${n.fromTimestamp},
                "property": "${n.label}",
                ${parseLocationToWKT(n.getProps(name = LOCATION).firstOrNull(), isUpdate = true)}
                ${relationshipToAsterixCitizen(n.relationships)}
                ${propertiesToAsterixCitizen(n.getProps())}
                "fromTimestamp": datetime("${timestampToISO8601(n.fromTimestamp)}"),
                "toTimestamp": datetime("${timestampToISO8601(n.toTimestamp)}"),
                "value": ${n.value}
            }]);
        """.trimIndent()
        asterixHTTPClient.updateTs(upsertStatement)
    }

    private fun selectNodeFromJsonObject(node: JSONObject): N {
        val entity = CustomVertex(
            id = encodeBitwise(getTSId(), node.getInt("timestamp").toLong()),
            type = labelFromString(node.getString("property")),
            fromTimestamp = dateToTimestamp(node.getString("fromTimestamp")),
            toTimestamp = dateToTimestamp(node.getString("toTimestamp")),
            value = node.getDouble("value").toLong(),
            g = g
        )
        node.takeIf { it.has("relationships") }
            ?.getJSONArray("relationships")
            ?.let { array -> List(array.length()) { array.getJSONObject(it) } }
            ?.map { jsonToRel(it, g) }
            ?.let(entity.relationships::addAll)

        node.takeIf { it.has("properties") }
            ?.getJSONArray("properties")
            ?.let { array -> List(array.length()) { array.getJSONObject(it) } }
            ?.map { jsonToProp(it, g) }
            ?.let(entity.properties::addAll)
        return entity
    }

}