package it.unibo.graph

import it.unibo.graph.structure.CustomGraph
import org.apache.commons.lang3.NotImplementedException
import org.locationtech.jts.geom.Geometry
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

class Compare(val a: String, val b: String, val property: String, val operator: Operators) {
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

class Aggregate(val n: String, val property: String? = null, val operator: AggOperator? = null)

@JvmName("query")
fun query(match: List<Step?>, where: List<Compare> = listOf(), by: List<Aggregate> = listOf(), from: Long = Long.MIN_VALUE, to: Long = Long.MAX_VALUE, timeaware: Boolean = false): List<Any> {
    return query(listOf(match), where, by, from, to, timeaware)
}

@JvmName("queryJoin")
fun query(match: List<List<Step?>>, where: List<Compare> = listOf(), by: List<Aggregate> = listOf(), from: Long = Long.MIN_VALUE, to: Long = Long.MAX_VALUE, timeaware: Boolean = false): List<Any> {
    val mapAliases: Map<String, Pair<Int, Int>> =
        match // get the aliases for each pattern
            .mapIndexed { patternIndex, pattern ->
                pattern
                    .mapIndexed { a, b -> Pair(a, b) }
                    .filter { it.second?.alias != null } // Consider only the steps with aliases
                    .associate { it.second?.alias!! to Pair(patternIndex, it.first) } // Pair(patternIndex, stepIndex)
            }
            .reduce { acc, map -> acc + map }
    val joinFilters = where.filter { mapAliases[it.a]!!.first != mapAliases[it.b]!!.first } // Take the filters that refer to different patterns (i.e., filters for joining the patterns)
    val pattern1 = search(match[0], (where - joinFilters).filter { mapAliases[it.a]!!.first == 0 }, from, to, timeaware) // compute the first pattern by removing all join filters and consider only the filters on the first pattern (i.e., patternIndex = 0)
    return if (match.size == 1) {
            pattern1
        } else {
            pattern1.map { path -> // for each result (i.e., path) of the first pattern
                var discard = false  // whether this path should be discarded because it will not match
                // MATCH (a), (b) WHERE a.name = b.name
                // RESULT of the first pattern, consider two nodes/paths ({name: "Alice"}), ({surname: "Black"})
                val curMatch = match[1].map { step -> // push down the predicates given the values of the first path
                    step?.let { // for each step of the second pattern; e.g. try ({name: "Alice"}) first and then ({surname: "Black"})
                        val t = joinFilters // let's consider the joining filters; e.g. a.name = b.name
                            .filter { it.a == step.alias || it.b == step.alias } // take only the joining filters referring to the current step
                            .map {
                                val cAlias = if (step.alias != it.a) it.a else it.b // take the alias of the step in the previous pattern; e.g. "a"
                                val props = path[mapAliases[cAlias]!!.second].getProps(name = it.property) // take the property of the corresponding node/edge in the path; e.g. "a.name"
                                if (props.isEmpty()) { // if the node has no such property, then this path can be discarded before the join // TODO, durante la ricerca dei path potrei già scartarlo tra i risultati possibili?
                                    discard = true
                                    Triple(it.property, it.operator, "foo")
                                } else {
                                    Triple(it.property, it.operator, props[0].value) // else, add this as a filter
                                }
                            }
                        Step(step.type, it.properties + t, step.alias) // ... and extend the step; e.g., Step("B", alias="b") becomes Step("B", alias="b", property=("name", EQ, "Alice")
                    }
                }
                if (discard) { // if the path is discarded, do not perform the search
                    emptyList()
                } else { // else, search for valid paths for the second pattern
                    val pattern2 = search(curMatch, (where - joinFilters).filter { mapAliases[it.a]!!.first == 1 }, from, to, timeaware)
                        .map { path + it }
                    pattern2
                }
            }
            .flatten()
        }
        .flatMap { row ->
            // Find the nodes that should be replaced by their properties
            // MATCH (n)-->(m) RETURN n.name, m => only n
            // MATCH (n)-->(m) RETURN n.name, avg(m.value) => n and m
            val toReplace = by.filter { it.property != null }
            // accumulator of rows
            var acc: MutableList<List<Any>> = mutableListOf(row)
            toReplace.forEach { by -> // for each element (e.g., node or edge) to replace
                val alias = mapAliases[by.n]!!
                val index = alias.second + (if (alias.first == 1) match[0].size else 0) // find its index in the path
                    val acc2: MutableList<List<Any>> = mutableListOf() // current accumulator
                    acc.forEach { row -> // for each row
                    val props = (row[index] as ElemP).getProps(name = by.property) // find the property to replace
                    if (props.isEmpty()) { // if the element does not contain the property...
                        acc2.add(row.subList(0, index) + listOf("null") + row.subList(index + 1, row.size)) // add a null value
                    } else {
                        props.forEach {  // else, for each matching property
                            p -> acc2.add(row.subList(0, index) + listOf(p.value) + row.subList(index + 1, row.size)) // produce a new row with the replaced value
                        }
                    }
                }
                acc = acc2 // iterate over the new rows
            }
            acc
        }
        .groupBy { row ->
            // group by all elements that are not aggregation operator
            // MATCH (n)-->(m) RETURN n.name, m.name => group by n and m
            // MATCH (n)-->(m) RETURN n.name, avg(m.value) => group by only on n
            by.filter { it.operator == null }.map {
                val alias = mapAliases[it.n]!!
                row[alias.second + (if (alias.first == 1) match[0].size else 0)]
            }
        }
        .mapValues { group: Map.Entry<List<Any>, List<List<Any>>> ->
            if (!by.any { it.operator != null }) {
                // no aggregation operator has been specified
                group.value
            } else {
                // some aggregation operator has been specified
                // TODO apply different aggregation operators, and possibly multiple aggregation operators
                val value: Double = group.value.map { row -> (row[mapAliases[by.first { it.operator != null }.n]!!.second] as Number).toDouble()}.mean(true)
                listOf(value) // E.g., [12.5]
            }
        }
        .map {
            if (by.isNotEmpty()) {
                if (by.any { it.operator != null }) {
                    it.key + it.value // MATCH (n)-->(m) RETURN n.name, avg(m.value) => [[a, 12.5], [b, 13.0], ...]
                } else {
                    if (by.size == 1) {
                        it.key // MATCH (n) RETURN n.name => [a, b, ...]
                    } else {
                        listOf(it.key)  // MATCH (n)-->(m) RETURN n.name, m.name => [[a, b], [a, c], ...]
                    }
                }
            } else {
                it.value
            }
        }
        .flatten()
}

fun search(match: List<Step?>, where: List<Compare> = listOf(), from: Long = Long.MIN_VALUE, to: Long = Long.MAX_VALUE, timeaware: Boolean = false): List<List<ElemP>> {
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

    override fun addEdge(r: R): R {
        if (r.id >= rels.size || r.id == DUMMY_ID) {
            return super.addEdge(r)
        } else {
            rels[(r.id as Number).toInt()] = r
            return r
        }
    }

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
        // Find the last properties and update its toTimestamp
        // Update only if toTimestamp of old property is > than fromTimestamp of new property.
        props.filter { it.key == p.key }
            .maxByOrNull { it.toTimestamp }
            ?.let {
                if (it.toTimestamp > p.fromTimestamp) {
                    it.toTimestamp = p.fromTimestamp
                }
            }
        props += p
        return p
    }

    override fun nextEdgeId(): Int = rels.size

    override fun addEdgeLocal(key: Long, r: R): R {
        // Find the last edge version and update its toTimestamp
        // Update only if toTimestamp of old edge is > than fromTimestamp of new property.
        rels.filter { it.id == r.id }
            .maxByOrNull { it.toTimestamp }
            ?.let {
                if (it.toTimestamp > r.fromTimestamp) {
                    it.toTimestamp = r.fromTimestamp
                }
            }
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
        //TODO: close toTimestamp of previous property version
        db.put(properties, "${p.id}".toByteArray(), serialize(p))
        return p
    }

    override fun addEdgeLocal(key: Long, r: R): R {
        //TODO: close toTimestamp of previous edge version
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