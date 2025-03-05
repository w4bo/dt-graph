package it.unibo.graph

import it.unibo.graph.structure.CustomGraph
import org.apache.commons.lang3.NotImplementedException
import org.json.JSONObject
import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.geom.GeometryFactory
import org.locationtech.jts.geom.LinearRing
import org.locationtech.jts.io.geojson.GeoJsonReader
import org.rocksdb.*
import java.io.*
import kotlin.math.max
import kotlin.math.min
import org.jetbrains.kotlinx.dataframe.math.mean

// Serialize an object to byte array
fun serialize(obj: Serializable): ByteArray {
    ByteArrayOutputStream().use { bos ->
        ObjectOutputStream(bos).use { out -> out.writeObject(obj) }
        return bos.toByteArray()
    }
}

// Deserialize a byte array to an object
inline fun <reified T : Serializable> deserialize(bytes: ByteArray?): T {
    ByteArrayInputStream(bytes).use { bis ->
        ObjectInputStream(bis).use { `in` -> return `in`.readObject() as T }
    }
}

interface Elem: Serializable {
    val id: Number
    val fromTimestamp: Long
    var toTimestamp: Long
}

interface ElemP: Elem {
    val type: String
    var nextProp: Int?
    val properties: MutableList<P>
    fun getProps(next: Int? = nextProp, filter: PropType? = null, name: String? = null, fromTimestamp: Long = Long.MIN_VALUE, toTimestamp: Long = Long.MAX_VALUE): List<P> {
        fun filter(p: P): Boolean = (filter == null || p.type == filter) && (name == null || p.key == name) && !(p.fromTimestamp > toTimestamp || p.toTimestamp < fromTimestamp)
        return if (next == null) {
            properties.filter { filter(it) }
        } else {
            val p = App.g.getProp(next)
            (if (filter(p)) listOf(p) else emptyList()) + getProps(p.next, filter, name, fromTimestamp, toTimestamp)
        }
    }
}

interface IStep {
    val type: String?
    val properties: List<Triple<String, Operators, Any>>
    val alias: String?
}

class Step(
    override val type: String? = null,
    override val properties: List<Triple<String, Operators, Any>> = listOf(),
    override val alias: String? = null
) : IStep

enum class Operators { EQ, LT, GT, LTE, GTE, ST_CONTAINS }

enum class AggOperator { SUM, COUNT, AVG, MIN, MAX }

class Compare(val a: Int, val b: Int, val property: String, val operator: Operators) {
    private fun compareIfSameType(a: Any, b: Any, operator: Operators): Boolean {
        //if ( a::class != b::class) return false

        if (operator == Operators.EQ) return a == b

        if (a is Comparable<*> && b is Comparable<*>) {
            @Suppress("UNCHECKED_CAST")
            val compA = a as Comparable<Any>
            val compB = b as Any

            return when (operator) {
                Operators.LT -> compA < compB
                Operators.GT -> compA > compB
                Operators.LTE -> compA <= compB
                Operators.GTE -> compA >= compB
                Operators.ST_CONTAINS -> geometryContains(a, b)
                else -> false
            }
        }
        return false
    }

    private fun geometryContains(a: Any, b: Any): Boolean {
        val parser = GeoJsonReader()
        val geomA = when (a) {
            is Geometry -> a
            is String -> parser.read(a)
            else -> throw IllegalArgumentException("Invalid type for 'a': ${a::class.simpleName}")
        }
        val geomB = when (b) {
            is Geometry -> b
            is String -> parser.read(b)
            else -> throw IllegalArgumentException("Invalid type for 'b': ${b::class.simpleName}")
        }
        return geomA.contains(geomB)
    }

    fun isOk(a: ElemP, b: ElemP): Boolean {
        val p1 = a.getProps(name = property)
        val p2 = b.getProps(name = property)

        return p1.isNotEmpty() && p2.isNotEmpty() && compareIfSameType(p1[0].value, p2[0].value, operator)

    }
}

class Aggregate(val n: String, val property: String, val operator: AggOperator)

//fun search(match: List<List<Step?>>, where: List<Compare> = listOf(), by: List<Int>, agg: Aggregate, from: Long = Long.MIN_VALUE, to: Long = Long.MAX_VALUE, timeaware: Boolean = false): List<List<Any>> {
//    return search(match, where, from, to, timeaware)
//        .groupBy { row -> by.map { row[it] } }
//        .mapValues { it -> it.value.map{ (it[agg.n] as N).value?.toDouble() ?: Double.NaN }.mean(true) } // TODO apply different aggregation operators
//        .map { it.key + listOf(it.value as Any) }
//}

fun search(match: List<Step?>, where: List<Compare> = listOf(), by: List<Int>, agg: Aggregate, from: Long = Long.MIN_VALUE, to: Long = Long.MAX_VALUE, timeaware: Boolean = false): List<List<Any>> {
    val mapAlias: Map<String, Int> = match.mapIndexed { a, b -> Pair(a, b) }.filter { it.second?.alias != null }.associate { it.second?.alias!! to it.first }
    return search(match, where, from, to, timeaware)
        .groupBy { row -> by.map { row[it] } }
        .mapValues { it -> it.value.map{ (it[mapAlias[agg.n]!!] as N).value?.toDouble() ?: Double.NaN }.mean(true) } // TODO apply different aggregation operators
        .map { it.key + listOf(it.value as Any) }
}

fun search(match: List<Step?>, where: List<Compare> = listOf(), from: Long = Long.MIN_VALUE, to: Long = Long.MAX_VALUE, timeaware: Boolean = false): MutableList<List<ElemP>> {
    val visited: MutableSet<Number> = mutableSetOf()
    val acc: MutableList<List<ElemP>> = mutableListOf()
    val mapWhere: Map<String, Compare> = where.associateBy { it.b }
    val mapAlias: Map<String, Int> = match.mapIndexed { a, b -> Pair(a, b) }.filter { it.second?.alias != null }.associate { it.second?.alias!! to it.first }

    fun timeOverlap(it: Elem, from: Long, to: Long): Boolean = !timeaware || !(to < it.fromTimestamp || from > it.toTimestamp)

    fun dfs(e: ElemP, index: Int, path: List<ElemP>, from: Long, to: Long) {
        val alias: String? = match[index]?.alias
        val c: Compare? = if (alias != null) mapWhere[alias] else null
        if ((match[index] == null || ( // no filter
                (match[index]!!.type == null || match[index]!!.type == e.type)  // filter on label
                && match[index]!!.properties.all { f -> e.getProps(name = f.first, fromTimestamp = from, toTimestamp = to).any { p -> p.value == f.third }})) // filter on properties, TODO should implement different operators
            && timeOverlap(e, from, to) // check time overlap
            && (c == null || c.isOk(path[mapAlias[c.a]!!], e)) // apply the where clause
        ) {
            val curPath = path + listOf(e)
            if (curPath.size == match.size) {
                acc.add(curPath)
            } else {
                if (visited.contains(e.id)) {
                    return
                }
                val from = max(e.fromTimestamp, from)
                val to = min(e.toTimestamp, to)
                if (index % 2 == 0) { // is node
                    visited += e.id
                    (e as N)
                        .getRels(direction = Direction.OUT, includeHasTs = true)
                        .forEach {
                            dfs(it, index + 1, curPath, from, to)
                        }
                } else { // is edge...
                    val r = (e as R)
                    if (e.type == HAS_TS) { // ... to time series
                        App.tsm
                            .getTS(r.toN)
                            .getValues()
                            .forEach {
                                dfs(it, index + 1, curPath, from, to)
                            }
                    } else { // ... or to graph node
                        val n = App.g.getNode(r.toN)
                        dfs(n, index + 1, curPath, from, to)
                    }
                }
            }
        }
    }
    for (node in App.g.getNodes()) {
        dfs(node, 0, emptyList(), from, to)
    }
    return acc
}

interface Graph {
    fun clear()
    fun createNode(label: String, value: Long? = null, id: Long = nextNodeIdOffset(), from: Long, to: Long): N = N(id, label, value = value, fromTimestamp = from, toTimestamp = to)
    fun nextNodeIdOffset(): Long = encodeBitwise(GRAPH_SOURCE, nextNodeId())
    fun nextNodeId(): Long
    fun addNode(label: String, value: Long? = null, from: Long = Long.MIN_VALUE, to: Long = Long.MAX_VALUE): N = addNode(createNode(label, value, from = from, to = to))
    fun addNode(n: N): N
    fun createProperty(sourceId: Long, sourceType: Boolean, key: String, value: Any, type: PropType, id: Int = nextPropertyId(), from: Long, to: Long): P = P(id, sourceId, sourceType, key, value, type)
    fun nextPropertyId(): Int
    fun addProperty(sourceId: Long, key: String, value: Any, type: PropType, from: Long = Long.MIN_VALUE, to: Long = Long.MAX_VALUE, sourceType: Boolean = NODE, id: Int = nextPropertyId()): P = addProperty(createProperty(sourceId, sourceType, key, value, type, from = from, to = to, id=id))
    fun upsertFirstCitizenProperty(prop: P): P?

    fun addProperty(p: P): P {
        val (source, key) = decodeBitwise(p.sourceId)
        return if (source == GRAPH_SOURCE) {
            when (p.key){
                LOCATION -> {
                    upsertFirstCitizenProperty(p)
                    addPropertyLocal(key, p)
                }
                else -> addPropertyLocal(key, p)
            }
        } else {
            addPropertyTS(source, key, p)
        }
    }

    fun addPropertyLocal(key: Long, p: P): P
    fun addPropertyTS(tsId: Long, key: Long, p: P): P {
        if (p.sourceType == NODE) {
            val ts = App.tsm.getTS(tsId)
            val n = ts.get(key)
            n.properties += p
            ts.add(n)
            return p
        } else {
            throw IllegalArgumentException("Cannot add property to edge in TS")
        }
    }

    fun createEdge(label: String, fromNode: Long, toNode: Long, id: Int = nextEdgeId(), from: Long, to: Long): R = R(id, label, fromNode, toNode, fromTimestamp = from, toTimestamp = to)
    fun nextEdgeId(): Int
    fun addEdge(r: R): R {
        val (source, key) = decodeBitwise(r.fromN)
        return if (source == GRAPH_SOURCE) {
            addEdgeLocal(key, r)
        } else {
            addEdgeTS(source, key, r)
        }
    }

    fun addEdgeLocal(key: Long, r: R): R
    fun addEdgeTS(tsId: Long, key: Long, r: R): R {
        val ts = App.tsm.getTS(tsId)
        val n = ts.get(key)
        n.relationships += r
        ts.add(n)
        return r
    }

    fun addEdge(label: String, fromNode: Long, toNode: Long, id: Int = nextEdgeId(), from: Long = Long.MIN_VALUE, to: Long = Long.MAX_VALUE): R = addEdge(createEdge(label, fromNode, toNode, id=id, from, to))
    fun getProps(): MutableList<P>
    fun getNodes(): MutableList<N>
    fun getEdges(): MutableList<R>
    fun getProp(id: Int): P
    fun getNode(id: Long): N
    fun getEdge(id: Int): R
}

fun encodeBitwise(x: Long, y: Long, offset: Int = 44, mask: Long = 0xFFFFFFFFFFF): Long {
    return (x shl offset) or (y and mask)
}

fun decodeBitwise(z: Long, offset: Int = 44, mask: Long = 0xFFFFFFFFFFF): Pair<Long, Long> {
    val x = (z shr offset)
    val y = (z and mask)
    return Pair(x, y)
}

fun decodeBitwiseSource(z: Long, offset: Int = 44): Long {
    return z shr offset
}

open class GraphMemory: Graph {
    private val nodes: MutableList<N> = ArrayList()
    private val rels: MutableList<R> = ArrayList()
    private val props: MutableList<P> = ArrayList()

    override fun clear() {
        nodes.clear()
        rels.clear()
        props.clear()
    }

    override fun nextNodeId(): Long = nodes.size.toLong()

    override fun addNode(n: N): N {
        if (n.id >= nodes.size) {
            nodes += n
        } else {
            nodes[(n.id as Number).toInt()] = n
        }
        return n
    }

    override fun nextPropertyId(): Int = props.size

    override fun upsertFirstCitizenProperty(prop: P): P? {
        val nodeId = prop.sourceId
        nodes[nodeId.toInt()].location = GeoJsonReader().read(prop.value.toString())
        nodes[nodeId.toInt()].locationTimestamp = prop.fromTimestamp
        return prop
    }

    override fun addPropertyLocal(key: Long, p: P): P {
        props.find{ it.key == p.key}?.let{
            it.toTimestamp = p.fromTimestamp-1//System.currentTimeMillis()
        }
        props += p
        return p
    }

    override fun nextEdgeId(): Int = rels.size

    override fun addEdgeLocal(key: Long, r: R): R {
        rels += r
        return r
    }

    override fun getProps(): MutableList<P> {
        return props
    }

    override fun getNodes(): MutableList<N> {
        return nodes
    }

    override fun getEdges(): MutableList<R> {
        return rels
    }

    override fun getProp(id: Int): P {
        return props[id]
    }

    override fun getNode(id: Long): N {
        return nodes[(id as Number).toInt()]
    }

    override fun getEdge(id: Int): R {
        return rels[id]
    }
}

class GraphRocksDB : Graph {
    val nodes: ColumnFamilyHandle
    val edges: ColumnFamilyHandle
    val properties: ColumnFamilyHandle
    val db: RocksDB
    var nodeId = 0L
    var edgeId = 0
    var propId = 0
    val DB_NAME = "db_graph"

    init {
        val options = DBOptions()
        options.setCreateIfMissing(true)
        options.setCreateMissingColumnFamilies(true)
        val cfDescriptors = listOf(
            ColumnFamilyDescriptor("default".toByteArray(), ColumnFamilyOptions()),
            ColumnFamilyDescriptor("nodes".toByteArray(), ColumnFamilyOptions()),
            ColumnFamilyDescriptor("edges".toByteArray(), ColumnFamilyOptions()),
            ColumnFamilyDescriptor("properties".toByteArray(), ColumnFamilyOptions())
        )
        val cfHandles: List<ColumnFamilyHandle> = ArrayList()
        db = RocksDB.open(options, DB_NAME, cfDescriptors, cfHandles)
        nodes = cfHandles[1]
        edges = cfHandles[2]
        properties = cfHandles[3]
    }

    override fun clear() {
         listOf(nodes, edges, properties).forEach {
             val iterator = db.newIterator(it)
             iterator.seekToFirst()
             while (iterator.isValid) {
                 db.delete(it, iterator.key())
                 iterator.next()
             }
         }
        nodeId = 0
        edgeId = 0
        propId = 0
    }

    override fun nextNodeId(): Long = nodeId++

    override fun nextPropertyId(): Int = propId++
    override fun upsertFirstCitizenProperty(prop: P): P? {
        TODO("Not yet implemented")
    }

    override fun nextEdgeId(): Int = edgeId++

    override fun addNode(n: N): N {
        db.put(nodes, "${n.id}".toByteArray(), serialize(n))
        return n
    }

    override fun addPropertyLocal(key: Long, p: P): P {
        db.put(properties, "${p.id}".toByteArray(), serialize(p))
        return p
    }

    override fun addEdgeLocal(key: Long, r: R): R {
        db.put(edges, "${r.id}".toByteArray(), serialize(r))
        return r
    }

    override fun getProps(): MutableList<P> {
        throw NotImplementedException()
    }

    override fun getNodes(): MutableList<N> {
        val acc: MutableList<N> = mutableListOf()
        val iterator = db.newIterator(nodes)
        iterator.seekToFirst()
        while (iterator.isValid) {
            acc += deserialize<N>(iterator.value())
            iterator.next()
        }
        return acc
    }

    override fun getEdges(): MutableList<R> {
        throw NotImplementedException()
    }

    override fun getProp(id: Int): P {
        val b = db.get(properties, "$id".toByteArray())
        return deserialize<P>(b)
    }

    override fun getNode(id: Long): N {
        val b = db.get(nodes, "$id".toByteArray())
        return deserialize<N>(b)
    }

    override fun getEdge(id: Int): R {
        val b = db.get(edges, "$id".toByteArray())
        return deserialize<R>(b)
    }
}

object App {
    //val tsm = MemoryTSManager()
    val g: CustomGraph = CustomGraph(GraphMemory())
    val tsm = AsterixDBTSM.createDefault()
    // val tsm = RocksDBTSM()
    // val g: CustomGraph = CustomGraph(GraphRocksDB())
}