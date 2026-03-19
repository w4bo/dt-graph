package it.unibo.graph.asterixdb

import it.unibo.graph.interfaces.Graph
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
import java.io.PrintWriter

@OptIn(DelicateCoroutinesApi::class)
class AsterixDBTS(
    override val g: Graph,
    val id: Long,
    // clusterControllerHost: String,
    // nodeControllersIPs: List<String>,
    private val dataverse: String,
    // datatype: String,
    // get : Boolean = false,
    //val dataset: String,
    val asterixHTTPClient: AsterixDBHTTPClient,
    val writer: PrintWriter
) : TS {


    //val dataFeedIp : String = getDataFeedIP(id, nodeControllersIPs)
    //var dataFeedPort : Int = incPort()
    //private lateinit var socket: Socket
    //private lateinit var outputStream: OutputStream
    //private lateinit var writer: PrintWriter
    //private var isFeedConnectionOpen: Boolean = false

    //private fun getDataFeedIP(id: Long, ipList: List<String>): String {
    //    val hash = id.hashCode()
    //    val index = (hash and 0x7FFFFFFF) % ipList.size
    //    return ipList[index]
    //}

    //fun openDataFeedConnection() {
    //    socket = Socket(dataFeedIp, dataFeedPort)
    //    outputStream = socket.getOutputStream()
    //    writer = PrintWriter(outputStream, true)
    //    isFeedConnectionOpen = true
    //}

    //fun closeDataFeedConnection(closeRemote: Boolean = false) {
    //    writer.close()
    //    outputStream.close()
    //    socket.close()
    //    isFeedConnectionOpen = false
    //    if (closeRemote) {
    //        asterixHTTPClient.stopFeed()
    //        asterixHTTPClient.dropFeed()
    //    }
    //}

    override fun getTSId(): Long = id

    override fun add(n: N, isUpdate: Boolean): N {
        // var closeFeed = false
        if (isUpdate) {
            updateTs(n)
        } else {
            // if (!isFeedConnectionOpen) {
            //     openDataFeedConnection()
            //     closeFeed = true
            // }
            val event = nodeToJson(n, isUpdate = false, tsId = id)
            writer.println(event)
            if (writer.checkError()) {
                throw Exception("Couldn't insert data into AsterixDB")
            }
            // if (closeFeed) {
            //     closeDataFeedConnection()
            // }
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
        }
    }

    override fun getValues(by: List<Aggregate>, filters: List<Filter>, isGroupBy: Boolean): List<N> {
        var selectQuery: String

        val filters: List<Pair<String, Filter>> = (filters + Filter(TSID, Operators.EQ, id))
                .map { if (it.property == FROM_TIMESTAMP || it.property == TO_TIMESTAMP) Filter(ID, it.operator, it.value, it.attrFirst) else it } // rename id into TO/FROM TIMESTAMP
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
        groupby += filters
            .filter { f -> !by.map { it.property }.contains(f.second.property) } // cannot select an attribute that I should also aggregate (e.g., SELECT value, SUM(value) ... GROUP BY value)
            .filter { it.first != ID }.map { (orig, _) -> orig }
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
                    FROM ${asterixHTTPClient.dataset}
                    ${applyFilters(filters.map { it.second })}
                    GROUP BY ${groupby.joinToString(","){ removeAlias(it).replace(".$VALUE", "") }}
                """.trimIndent()
        } else {
            selectQuery = """
                    SELECT *
                    FROM ${asterixHTTPClient.dataset}
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
                outNodes = result.entities.map { jsonToNode(tsId = id, g = g, node = (it as JSONObject).getJSONObject(asterixHTTPClient.dataset)) }
                return outNodes
            }
            is AsterixDBResult.GroupByResult -> {
                if (result.entities.isEmpty) return emptyList()
                outNodes =  result.entities
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
                return outNodes
            }
            else -> throw UnsupportedOperationException(selectQuery)
        }
    }

    override fun get(eventId: Long): N {
        val selectQuery = "USE $dataverse; SELECT * FROM ${asterixHTTPClient.dataset} WHERE $TSID = $id AND $ID = $eventId"
        when (val result = asterixHTTPClient.selectFromAsterixDB(selectQuery)) {
            is AsterixDBResult.SelectResult -> {
                if (result.entities.length() > 0) {
                    return jsonToNode(tsId = id, g = g, node = (result.entities[0] as JSONObject).get(asterixHTTPClient.dataset) as JSONObject)
                } else {
                    throw IllegalArgumentException("Result is empty")
                }
            }

            else -> throw IllegalArgumentException("Error occurred while performing query \n $selectQuery")
        }
    }

    private fun updateTs(n: N) = asterixHTTPClient.updateTs("USE $dataverse; UPSERT INTO ${asterixHTTPClient.dataset} ([${nodeToJson(n, isUpdate = true, tsId = id)}])")
}