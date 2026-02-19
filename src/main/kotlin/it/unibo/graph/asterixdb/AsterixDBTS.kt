package it.unibo.graph.asterixdb

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import it.unibo.graph.interfaces.*
import it.unibo.graph.query.AggOperator
import it.unibo.graph.query.Aggregate
import it.unibo.graph.query.Filter
import it.unibo.graph.utils.*
import kotlinx.coroutines.DelicateCoroutinesApi
import org.json.JSONObject
import java.io.OutputStream
import java.io.PrintWriter
import java.net.Socket
import java.nio.file.Files
import java.nio.file.Paths

@OptIn(DelicateCoroutinesApi::class)
class AsterixDBTS(
    override val g: Graph,
    val id: Long,
    clusterControllerHost: String,
    nodeControllersIPs: List<String>,
    private val dataverse: String,
    datatype: String,
    seed: Long = id,
    get : Boolean = false
) : TS {

    private var dataset: String

    private val asterixHTTPClient: AsterixDBHTTPClient
    val dataFeedIp : String = getDataFeedIP(id, nodeControllersIPs)
    var dataFeedPort : Int = getDataFeedPort(id, FIRSTFEEDPORT, LASTFEEDPORT)

    private lateinit var socket: Socket
    private lateinit var outputStream: OutputStream
    private lateinit var writer: PrintWriter
    private var isFeedConnectionOpen: Boolean = false
    private val mapper = ObjectMapper()

    init {
        asterixHTTPClient = AsterixDBHTTPClient(
            clusterControllerHost,
            dataFeedIp,
            dataFeedPort,
            dataverse,
            datatype,
            id,
            seed,
            get = get
        )

        dataset = asterixHTTPClient.getDataset()
        // DataFeedPort might be changed if connection failed
        dataFeedPort = asterixHTTPClient.getDataFeedPort()

    }

    private fun openDataFeedConnection() {
        socket = Socket(dataFeedIp, dataFeedPort)
        outputStream = socket.getOutputStream()
        writer = PrintWriter(outputStream, true)
        isFeedConnectionOpen = true
    }

    private fun closeDataFeedConnection(){
        writer.close()
        outputStream.close()
        socket.close()
        isFeedConnectionOpen = false
    }

    private fun getDataFeedIP(id: Long, ipList: List<String>): String {
        val hash = id.hashCode()
        val index = (hash and 0x7FFFFFFF) % ipList.size
        return ipList[index]
    }

    private fun getDataFeedPort(id: Long, min: Int, max: Int): Int {
        require(min <= max) { "min must be <= max" }
        val rangeSize = max - min + 1
        val normalized = (id % rangeSize).toInt()
        return min + normalized
    }

    fun loadInitialData(path: String) {
        if (asterixHTTPClient.isDatasetEmpty(dataverse, dataset)) {
            val projectRoot = Paths.get("").toAbsolutePath().normalize()
            val filePath = projectRoot.resolve(path).normalize()

            if (Files.exists(filePath)) {
                openDataFeedConnection()
                Files.newBufferedReader(filePath).useLines { lines ->
                    for (line in lines) {
                        if (line.isNotBlank()) {
                            val json: JsonNode = mapper.readTree(line)
                            val label = if (json.get(TYPE).textValue() == "Temperature")
                                "temperature" else "presence"

                            add(
                                label = labelFromString(json.get(TYPE).textValue()),
                                timestamp = dateToTimestamp(json.get("timestamp").textValue()),
                                location = json.get("location").textValue(),
                                // TODO: attualmente è fissato sulla sintassi SmartBench
                                value = json.get("payload").get(label).longValue(),
                                isUpdate = false
                            )
                        }
                    }
                }
                closeDataFeedConnection()
            } else {
                println("File not found: $filePath")
            }
        }
    }
    override fun getTSId(): Long = id

    override fun add(n: N, isUpdate: Boolean): N {
        var closeFeed = false
        if (isUpdate) {
            updateTs(n)
        } else {
            if (!isFeedConnectionOpen) {
                openDataFeedConnection()
                closeFeed = true
            }
            val event = """{
                "$ID": ${n.fromTimestamp},
                "$LABEL": "${n.label}",
                ${parseLocationToWKT(n.getProps(name = LOCATION).firstOrNull())}
                ${edgesToAsterixCitizen(n.edges)}
                ${propertiesToAsterixCitizen(n.properties)}
                "$VALUE": ${n.value?: 0}
            }""".trimIndent()// .replace("\\s+".toRegex(), " ")
            writer.println(event)
            if (writer.checkError()) {
                throw Exception("Couldn't insert data into AsterixDB")
            }
            if (closeFeed) {
                closeDataFeedConnection()
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
                        val prop = if (it.property == VALUE) "`$VALUE`" else it.property
                        "${AggOperator.SUM.name}($prop) as $prop"
                    }

                val groupbyClause = by.filter { it.operator == null }.mapNotNull { it.property }
                return Pair(selectClause, groupbyClause)
            }
        }
    }

    override fun getValues(by: List<Aggregate>, filters: List<Filter>, isGroupBy: Boolean): List<N> {
        var selectQuery: String
        val groupByClause: List<String>

        val defaultAggregatorsList: List<String> = listOf(ID, LABEL, "`$VALUE`", PROPERTIES, EDGES)
        val defaultGroupByAggregators: List<String>  = listOf(LABEL, "COUNT(*) as count", "MIN($ID) as $FROM_TIMESTAMP", "MAX($ID) as $TO_TIMESTAMP")

        val filters = filters.map { if (it.property == FROM_TIMESTAMP || it.property == TO_TIMESTAMP) Filter(ID, it.operator, it.value, it.attrFirst) else it }

        val whereAggregators: List<String> = filters
            .filter { f -> f.property != VALUE }
            .filter { f -> defaultAggregatorsList.none { it.contains(f.property) }  }
            .map { f -> f.property }

        if (by.isEmpty()) {
            // If not group by, add mandatory attributes for graph representation
            selectQuery = """
                    USE $dataverse;
                    SELECT ${(defaultAggregatorsList + whereAggregators).joinToString(",")}
                    FROM $dataset
                    ${applyFilters(filters)}
                """.trimIndent()
            if (isGroupBy) {
                selectQuery = "$selectQuery LIMIT 1;"
            }
        } else {
            val groupBy = groupby(by)!!
            val selectClause = groupBy.first
            groupByClause = groupBy.second
            // GROUP BY property + all GROUP BY predicates + WHERE predicates
            selectQuery = """
                USE $dataverse;
                SELECT ${(groupByClause + selectClause + defaultGroupByAggregators + whereAggregators).joinToString(",")}
                FROM $dataset
                ${applyFilters(filters)}
                GROUP BY ${(groupByClause + listOf(LABEL) + whereAggregators).joinToString(",")};
            """.trimIndent()
        }
        val outNodes : List<N>
        when (val result = asterixHTTPClient.selectFromAsterixDB(selectQuery, isGroupBy = by.isNotEmpty())) {

            // If it's a simple select *
            is AsterixDBResult.SelectResult -> {
                if (result.entities.isEmpty) return emptyList()
                outNodes = result.entities.map { selectNodeFromJsonObject((it as JSONObject)) }
                return outNodes
            }

            // If we are aggregating
            is AsterixDBResult.GroupByResult -> {
                if (result.entities.isEmpty) return emptyList()
                val aggOperator = by.first { it.operator != null }.property!!
                val fromTimestamp = (0 until result.entities.length()).minOf {
                    result.entities.getJSONObject(it).getLong(FROM_TIMESTAMP)
                }
                val toTimestamp = (0 until result.entities.length()).maxOf {
                    result.entities.getJSONObject(it).getLong(TO_TIMESTAMP)
                }
                outNodes =  result.entities.toList().map { it as HashMap<*, *> }.map {
                    val aggValue = Pair((it[aggOperator] as? Number)?.toDouble(), (it["count"] as? Number)?.toDouble())
                    N.createVirtualN(
                        labelFromString(it[LABEL]!!.toString()),
                        aggregatedValue = aggValue,
                        fromTimestamp = fromTimestamp,
                        toTimestamp = toTimestamp,
                        g = g,
                        properties = it.keys.filter { key -> key != aggOperator && !defaultAggregatorsList.contains(key.toString()) }
                            .map { key -> parseProp(it, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, key = key.toString(), g = g) } + (
                                if (aggOperator != VALUE) {
                                    listOf(P(DUMMY_ID, sourceId = DUMMY_ID, key = aggOperator, value = aggValue, type = PropType.STRING, sourceType = NODE, g = g, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp))
                                } else {
                                    emptyList()
                                }
                            )
                    )
                }
                return outNodes
            }
            else -> {
                println("Error occurred while performing query \n $selectQuery")
                return emptyList()
            }
        }
    }

    override fun get(id: Long): N {
        val selectQuery = """
            USE $dataverse;
            SELECT * FROM $dataset
            WHERE $ID = $id
        """.trimIndent()
        when (val result = asterixHTTPClient.selectFromAsterixDB(selectQuery)) {
            is AsterixDBResult.SelectResult -> {
                if (result.entities.length() > 0) {
                    return selectNodeFromJsonObject((result.entities[0] as JSONObject).get(dataset) as JSONObject)
                } else {
                    throw IllegalArgumentException("Result is empty")
                }
            }

            else -> {
                throw IllegalArgumentException("Error occurred while performing query \n $selectQuery")
            }
        }
    }

    private fun updateTs(n: N) {
        val upsertStatement = """
            USE $dataverse;
            UPSERT INTO $dataset ([{
                "$ID": ${n.fromTimestamp},
                "$LABEL": "${n.label}",
                ${parseLocationToWKT(n.getProps(name = LOCATION).firstOrNull(), isUpdate = true)}
                ${edgesToAsterixCitizen(n.edges)}
                ${propertiesToAsterixCitizen(n.properties)}
                "$VALUE": ${n.value}
            }]);""".trimIndent()
        asterixHTTPClient.updateTs(upsertStatement)
    }

    private fun selectNodeFromJsonObject(node: JSONObject): N {
        val timestamp = node.getLong(ID)
        val id = encodeBitwise(getTSId(), timestamp)
        val entity = N(
            id = id,
            label = labelFromString(node.getString(LABEL)),
            fromTimestamp = timestamp,
            toTimestamp = timestamp,
            value = node.getLong(VALUE),
            g = g
        )
        node.takeIf { it.has(EDGES) }
            ?.getJSONArray(EDGES)
            ?.let { array -> List(array.length()) { array.getJSONObject(it) } }
            ?.map { jsonToEdge(it, fromTimestamp = timestamp, toTimestamp = timestamp, g) }
            ?.let(entity.edges::addAll)
        node.takeIf { it.has(PROPERTIES) }
            ?.getJSONArray(PROPERTIES)
            ?.let { array -> List(array.length()) { array.getJSONObject(it) } }
            ?.map { jsonToProp(it, id, NODE, fromTimestamp = timestamp, toTimestamp = timestamp, g) }
            ?.let(entity.properties::addAll)
        return entity
    }

}