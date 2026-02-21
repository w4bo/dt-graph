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
                                location = json.get(LOCATION).textValue(),
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
            val event = nodeToJson(n, isUpdate = false)
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

    val firstCitizens = listOf(ID, LABEL, VALUE, EDGES, LOCATION, FROM_TIMESTAMP, TO_TIMESTAMP)
    fun removeAlias(s: String): String {
        val index = s.indexOf(" as")
        return if (index == -1) s else s.substring(0, index)
    }

    fun replacePropertyName(property: String, alias: Boolean = true): String {
        return if (!firstCitizens.contains(property)) {
            "$property.$VALUE" + (if (alias) " as $property" else "")
        } else {
            property
        }
    }

    override fun getValues(by: List<Aggregate>, filters: List<Filter>, isGroupBy: Boolean): List<N> {
        var selectQuery: String

        val filters = filters
            .map { if (it.property == FROM_TIMESTAMP || it.property == TO_TIMESTAMP) Filter(ID, it.operator, it.value, it.attrFirst) else it }
            .map { f ->
                if (!firstCitizens.contains(f.property)) {
                    Pair(f.property ,  Filter(f.property + ".$VALUE", f.operator, f.value, f.attrFirst))
                } else {
                    Pair(f.property, f)
                }
            }

        val groupby: MutableSet<String> =
            by
                .filter { it.operator == null }
                .map { f -> f.property!! }
                .toMutableSet()
        groupby += listOf(LABEL)

        val aggregators: MutableSet<String> =
            by
                .filter { it.operator != null }
                .map { f -> "${(if (f.operator == AggOperator.AVG) AggOperator.SUM else f.operator)?.name}(${ replacePropertyName(f.property!!, alias = false) }) as ${f.property}" }.toMutableSet()
        aggregators += listOf("COUNT(*) as $COUNT", "MIN($ID) as $FROM_TIMESTAMP", "MAX($ID) as $TO_TIMESTAMP")

        selectQuery =  "USE $dataverse;"
        if (by.isNotEmpty()) {
            selectQuery +=  """
                    SELECT ${(groupby + aggregators).joinToString(",")}
                    FROM $dataset
                    ${applyFilters(filters.map { it.second })}
                    GROUP BY ${groupby.joinToString(","){ removeAlias(it).replace(".$VALUE", "") }}
                """.trimIndent()
        } else {
            selectQuery = """
                    SELECT *
                    FROM $dataset
                    ${applyFilters(filters.map { it.second })}
                """.trimIndent()
            if (isGroupBy) {
                selectQuery += " LIMIT 1"
            }
        }
        selectQuery = selectQuery.replace(VALUE, "`$VALUE`")
        val outNodes : List<N>
        when (val result = asterixHTTPClient.selectFromAsterixDB(selectQuery, isGroupBy = by.isNotEmpty())) {
            is AsterixDBResult.SelectResult -> {
                if (result.entities.isEmpty) return emptyList()
                outNodes = result.entities.map { jsonToNode(tsId = id, g = g, node = (it as JSONObject).getJSONObject(dataset)) }
                return outNodes
            }
            is AsterixDBResult.GroupByResult -> {
                if (result.entities.isEmpty) return emptyList()
                outNodes =  result.entities
                    .map { it as JSONObject }
                    .map { json ->
                        val aggOperator = by.first { it.operator != null }.property!!
                        // json.keys().forEach { key ->
                        //     val currentValue = json.get(key)
                        //     if (currentValue !is JSONObject) {
                        //         json.put(
                        //             key,
                        //             JSONObject().apply {
                        //                 put(VALUE, if(key != VALUE) currentValue else Pair((json[aggOperator] as? Number)?.toDouble(), (json[COUNT] as? Number)?.toDouble()))
                        //                 put(TYPE, PropType.NULL.ordinal)
                        //             }
                        //         )
                        //     }
                        // }
                        json.put(aggOperator, JSONObject().let { it ->
                            it.put(VALUE, Pair((json[aggOperator] as? Number)?.toDouble(), (json[COUNT] as? Number)?.toDouble()))
                            it.put(TYPE, PropType.NULL.ordinal)
                        })
                        json.remove(COUNT)
                        jsonToNode(tsId = id, g = g, node = json)
                    }
                return outNodes
            }
            else -> throw UnsupportedOperationException(selectQuery)
        }
    }

    override fun get(eventId: Long): N {
        val selectQuery = """
            USE $dataverse;
            SELECT * FROM $dataset WHERE $ID = $eventId
        """.trimIndent()
        when (val result = asterixHTTPClient.selectFromAsterixDB(selectQuery)) {
            is AsterixDBResult.SelectResult -> {
                if (result.entities.length() > 0) {
                    return jsonToNode(tsId = id, g = g, node = (result.entities[0] as JSONObject).get(dataset) as JSONObject)
                } else {
                    throw IllegalArgumentException("Result is empty")
                }
            }

            else -> throw IllegalArgumentException("Error occurred while performing query \n $selectQuery")
        }
    }

    private fun updateTs(n: N) = asterixHTTPClient.updateTs("""
            USE $dataverse;
            UPSERT INTO $dataset ([${nodeToJson(n, isUpdate = true)}])
        """.trimIndent())
}