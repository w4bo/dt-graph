package it.unibo.graph.asterixdb

import it.unibo.graph.interfaces.*
import it.unibo.graph.query.AggOperator
import it.unibo.graph.query.Aggregate
import it.unibo.graph.query.Filter
import it.unibo.graph.structure.CustomVertex
import it.unibo.graph.utils.*
import org.apache.tinkerpop.shaded.jackson.databind.annotation.JsonAppend.Prop
import org.json.JSONObject
import org.locationtech.jts.io.WKTReader

import java.io.OutputStream
import java.net.Socket
import java.io.PrintWriter

class AsterixDBTS(override val g: Graph, val id: Long, clusterControllerHost: String,  dataFeedIp: String, private val dataverse: String, datatype: String, busyPorts: MutableList<Int>, seed: Int = SEED): TS {

    private var dataset: String
    var dataFeedPort: Int

    private var socket: Socket
    private var outputStream: OutputStream
    private val writer: PrintWriter

    private val asterixHTTPClient : AsterixDBHTTPClient = AsterixDBHTTPClient(
        clusterControllerHost,
        dataFeedIp,
        busyPorts,
        g,
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
    }

    override fun getTSId(): Long = id

    override fun add(n: N, isUpdate: Boolean): N {
        if(isUpdate){
            updateTs(n)
        }else{
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

    private fun groupby(by:List<Aggregate>): Pair<List<String>,List<String>> ? {
        //TODO: ATM handles only SUM AggOperator
        return when {
            by.isEmpty() -> null
            else -> {
                val selectClause = by.filter { it.operator != null }
                    .map {
                        val prop = if (it.property == "value") "`value`" else it.property
                        "${AggOperator.SUM.name}($prop) as $prop"
                    }

                val groupbyClause = by.filter { it.operator == null }.mapNotNull {it.property}
                return Pair(selectClause, groupbyClause)
            }
        }
    }
    private fun parseProp(it: HashMap<*,*>, fromTimestamp:Long, toTimestamp:Long, key: String): P {
        fun parsePropType(key:String, value: Any): Pair<PropType, Any> {
            //TODO: I'm considering that only location could be a geometry
            return if(key == LOCATION){
                Pair(PropType.GEOMETRY, hashMapToGeometry(value as HashMap<String, Any>)!!)
            }else{
                when(value){
                    is Int -> Pair(PropType.INT, value.toInt())
                    is Long -> Pair(PropType.LONG, value.toLong())
                    is Double -> Pair(PropType.DOUBLE, value.toDouble())
                    else -> Pair(PropType.STRING, value.toString())
                }
            }
        }
        val propType = parsePropType(key, it[key]!!)
        return P(
            DUMMY_ID,
            sourceId = DUMMY_ID.toLong(),
            key = key,
            value =  propType.second,
            type = propType.first,
            sourceType = NODE,
            g = g,
            fromTimestamp = fromTimestamp,
            toTimestamp = toTimestamp
        )
    }
    override fun getValues(by: List<Aggregate>, filters: List<Filter>): List<N> {
        val selectQuery: String
        var groupByClause: List<String> = listOf()
        val defaultAggregatorsList = listOf(PROPERTY, FROM_TIMESTAMP, TO_TIMESTAMP)
        val defaultAggregators = ", $PROPERTY, COUNT(*) as count, MIN($FROM_TIMESTAMP) as $FROM_TIMESTAMP, MAX($TO_TIMESTAMP) as $TO_TIMESTAMP"
        if(by.isEmpty()){
            selectQuery = """
                    USE $dataverse;
                    SELECT *
                    FROM $dataset
                    ${applyFilters(filters)}
                """.trimIndent()
        }else{
            val groupBy = groupby(by)!!
            val selectClause = groupBy.first
            groupByClause = groupBy.second
            val groupByPredicates = groupByClause.joinToString(",")
            val whereAggregators = filters
                .filter { !defaultAggregators.contains(it.property) }
                .map { it.property }
                .joinToString(",", prefix = if (filters.any { !defaultAggregators.contains(it.property) }) "," else "")
            val groupByPart = if (groupByPredicates.isNotEmpty()) "GROUP BY $groupByPredicates, $PROPERTY" else "GROUP BY $PROPERTY $whereAggregators"

            selectQuery = """
                USE $dataverse;
                SELECT ${groupByClause.joinToString(",").let { if (it.isNotEmpty()) "$it," else "" }} ${selectClause.joinToString(",")} $defaultAggregators $whereAggregators
                FROM $dataset
                ${applyFilters(filters)}
                $groupByPart;
            """.trimIndent()
        }

        when (val result = asterixHTTPClient.selectFromAsterixDB(selectQuery, isGroupBy=by.isNotEmpty())) {

            // If it's a simple select *
            is AsterixDBResult.SelectResult -> {
                if (result.entities.isEmpty) return emptyList()
                return result.entities.map{ selectNodeFromJsonObject((it as JSONObject).get(dataset) as JSONObject)}
            }

            // If we are aggregating
            is AsterixDBResult.GroupByResult -> {
                if(result.entities.isEmpty) return emptyList()
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
                return result.entities.toList().map{it as HashMap<*,*>}.map{
                    val aggValue = Pair((it[aggOperator] as? Number)?.toDouble(), (it["count"] as? Number)?.toDouble())
                    N.createVirtualN(
                        Labels.valueOf(it[PROPERTY]!!.toString()),
                        aggregatedValue = aggValue,
                        fromTimestamp = fromTimestamp,
                        toTimestamp = toTimestamp,
                        g=g,
                        properties= it.keys.filter{key -> key != aggOperator && !defaultAggregatorsList.contains(key.toString())}.map{
                            key -> parseProp(it, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp, key = key.toString())
                        } + (
                            if(aggOperator != VALUE) {
                                listOf(P(DUMMY_ID, sourceId = DUMMY_ID.toLong(), key = aggOperator, value = aggValue, type = PropType.STRING, sourceType = NODE, g = g, fromTimestamp = fromTimestamp, toTimestamp = toTimestamp))
                            }else{
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
            fromTimestamp = dateToTimestamp(node.getString("fromTimestamp")) ,
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