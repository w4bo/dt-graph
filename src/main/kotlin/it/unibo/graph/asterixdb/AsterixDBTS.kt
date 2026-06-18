package it.unibo.graph.asterixdb

<<<<<<< HEAD
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
=======
import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.Label
import it.unibo.graph.interfaces.N
import it.unibo.graph.interfaces.PropType
import it.unibo.graph.interfaces.TS
import it.unibo.graph.query.AggOperator
import it.unibo.graph.query.Aggregate
import it.unibo.graph.query.Filter
import it.unibo.graph.query.Operators
import it.unibo.graph.utils.*
import kotlinx.coroutines.DelicateCoroutinesApi
import org.json.JSONObject
import java.io.BufferedWriter
import java.io.PrintWriter
>>>>>>> feat-tssingletable

@OptIn(DelicateCoroutinesApi::class)
class AsterixDBTS(
    override val g: Graph,
    val id: Long,
<<<<<<< HEAD
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
                            val label = if (json.get("type").textValue() == "Temperature")
                                "temperature" else "presence"

                            add(
                                label = labelFromString(json.get("type").textValue()),
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
            if(!isFeedConnectionOpen){
                openDataFeedConnection()
                closeFeed = true
            }

            val measurement = """
            {
                "timestamp": ${n.fromTimestamp},
                "property": "${n.label}",
                ${parseLocationToWKT(n.getProps(name = LOCATION).firstOrNull())}
                ${relationshipToAsterixCitizen(n.relationships)}
                ${propertiesToAsterixCitizen(n.getProps())}
                "value": ${n.value}
            }
            """
            writer.println(measurement)
            if (writer.checkError()) {
                println("Errore durante la scrittura nel file!")
                throw Exception("Coudln't insert data into AsterixDB")
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
                        val prop = if (it.property == "value") "`value`" else it.property
                        "${AggOperator.SUM.name}($prop) as $prop"
                    }

                val groupbyClause = by.filter { it.operator == null }.mapNotNull { it.property }
                return Pair(selectClause, groupbyClause)
            }
=======
    private val dataverse: String,
    private val dataset: String,
    val connection: AsterixDBHTTPClient
) : TS {

    override fun getTSId(): Long = id

    override fun add(n: N, isUpdate: Boolean, flush: Boolean): N {
        if (isUpdate) {
            updateTs(n)
        } else {
            val event = nodeToJson(n, isUpdate = false, tsId = id)
            connection.writeToFeed(event, flush)
        }
        return n
    }

    val firstCitizens = listOf(ID, LABEL, VALUE, EDGES, LOCATION, FROM_TIMESTAMP, TO_TIMESTAMP, TSID)
    fun removeAlias(s: String): String {
        val index = s.indexOf(" as")
        return if (index == -1) s else s.substring(0, index)
    }

    fun replacePropertyName(property: String, alias: Boolean = true): String {
        return if (!firstCitizens.contains(property)) {
            "$property.$VALUE" + (if (alias) " as $property" else "")
        } else {
            property
>>>>>>> feat-tssingletable
        }
    }

    override fun getValues(by: List<Aggregate>, filters: List<Filter>, isGroupBy: Boolean): List<N> {
        var selectQuery: String
<<<<<<< HEAD
        val groupByClause: List<String>
        val defaultAggregatorsList = listOf(PROPERTY, FROM_TIMESTAMP, TO_TIMESTAMP)
        val defaultGroupByAggregators = ", $PROPERTY, COUNT(*) as count, MIN($TIMESTAMP) as $FROM_TIMESTAMP, MAX($TIMESTAMP) as $TO_TIMESTAMP"

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
            if (isGroupBy) {
                selectQuery = "$selectQuery LIMIT 1;"
            }
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
                    result.entities.getJSONObject(
                        it
                    ).getLong(FROM_TIMESTAMP)
                }

                val toTimestamp = (0 until result.entities.length()).maxOf {
                    result.entities.getJSONObject(
                        it
                    ).getLong(TO_TIMESTAMP)
                }

                outNodes =  result.entities.toList().map { it as HashMap<*, *> }.map {
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
                                            sourceId = DUMMY_ID,
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
                return outNodes
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
        """
        when (val result = asterixHTTPClient.selectFromAsterixDB(selectQuery)) {
            is AsterixDBResult.SelectResult -> {
                if (result.entities.length() > 0) {
                    return selectNodeFromJsonObject((result.entities[0] as JSONObject).get(dataset) as JSONObject)
=======

        val filters: List<Pair<String, Filter>> = (filters + Filter(TSID, Operators.EQ, id))
                .map { if (it.property == FROM_TIMESTAMP || it.property == TO_TIMESTAMP) Filter(ID, it.operator, it.value, it.attrFirst) else it } // rename id into TO/FROM TIMESTAMP
                .map { if (it.property == LABEL) Filter(LABEL, it.operator, (it.value as Label).ordinal, it.attrFirst) else it } // rename id into TO/FROM TIMESTAMP
                .map { f ->
                        if (!firstCitizens.contains(f.property)) {
                            Pair(f.property,  Filter(f.property + ".$VALUE", f.operator, f.value, f.attrFirst))
                        } else {
                            Pair(f.property, f)
                        }
                    }

        val groupby: MutableSet<String> =
            by
                .filter { it.operator == null }
                .map { f -> f.property!! }
                .toMutableSet()
        groupby += filters
            .filter { f -> !by.map { it.property }.contains(f.second.property) } // cannot select an attribute that I should also aggregate (e.g., SELECT value, SUM(value) ... GROUP BY value)
            .filter { it.first != ID }.map { (orig, _) -> orig }
        // groupby += listOf(LABEL)

        val aggregators: MutableSet<String> =
            by
                .filter { it.operator != null }
                .map { f -> "${(if (f.operator == AggOperator.AVG) AggOperator.SUM else f.operator)?.name}(${ replacePropertyName(f.property!!, alias = false) }) as ${f.property}" }.toMutableSet()
        if (!groupby.any { it == LABEL })
            aggregators += listOf("MIN($LABEL) as $LABEL")
        aggregators += listOf("COUNT(*) as $COUNT", "MIN($ID) as $FROM_TIMESTAMP", "MAX($ID) as $TO_TIMESTAMP")

        selectQuery =  "USE $dataverse;"
        if (by.isNotEmpty()) {
            selectQuery +=  "SELECT ${(groupby + aggregators).joinToString(",")} FROM $dataset ${applyFilters(filters.map { it.second })} GROUP BY ${groupby.joinToString(","){ removeAlias(it).replace(".$VALUE", "") }}"
        } else {
            selectQuery += "SELECT * FROM $dataset ${applyFilters(filters.map { it.second })}"
            if (isGroupBy) {
                selectQuery += " LIMIT 1"
            }
        }
        selectQuery = selectQuery.replace(VALUE, "`$VALUE`")
        return when (val result = connection.selectFromAsterixDB(selectQuery, isGroupBy = by.isNotEmpty())) {
            is AsterixDBResult.SelectResult -> {
                if (result.entities.isEmpty) return emptyList()
                result.entities.map { jsonToNode(tsId = id, g = g, node = (it as JSONObject).getJSONObject(dataset)) }
            }
            is AsterixDBResult.GroupByResult -> {
                if (result.entities.isEmpty) return emptyList()
                result.entities
                    .map { it as JSONObject }
                    .map { json ->
                        val aggOperator = by.first { it.operator != null }.property!!
                        json.put(aggOperator, JSONObject().let {
                            it.put(VALUE, Pair((json[aggOperator] as? Number)?.toDouble(), (json[COUNT] as? Number)?.toDouble()))
                            it.put(TYPE, PropType.NULL.ordinal)
                        })
                        json.remove(COUNT)
                        jsonToNode(tsId = id, g = g, node = json)
                    }
            }
            else -> throw UnsupportedOperationException(selectQuery)
        }
    }

    override fun get(eventId: Long): N {
        val selectQuery = "USE $dataverse; SELECT * FROM $dataset WHERE $TSID = $id AND $ID = $eventId"
        when (val result = connection.selectFromAsterixDB(selectQuery)) {
            is AsterixDBResult.SelectResult -> {
                if (result.entities.length() > 0) {
                    return jsonToNode(tsId = id, g = g, node = (result.entities[0] as JSONObject).get(dataset) as JSONObject)
>>>>>>> feat-tssingletable
                } else {
                    throw IllegalArgumentException("Result is empty")
                }
            }

<<<<<<< HEAD
            else -> {
                throw IllegalArgumentException("Error occurred while performing query \n $selectQuery")
            }
        }
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
        """
        asterixHTTPClient.updateTs(upsertStatement)
    }

    private fun selectNodeFromJsonObject(node: JSONObject): N {
        val entity = N(
            id = encodeBitwise(getTSId(), node.getLong("timestamp")),
            label = labelFromString(node.getString("property")),
            fromTimestamp = node.getLong("timestamp"),
            toTimestamp = node.getLong("timestamp"),
            value = node.getLong("value"),
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

=======
            else -> throw IllegalArgumentException("Error occurred while performing query \n $selectQuery")
        }
    }

    private fun updateTs(n: N) = connection.updateTs("USE $dataverse; UPSERT INTO $dataset ([${nodeToJson(n, isUpdate = true, tsId = id)}])")
>>>>>>> feat-tssingletable
}