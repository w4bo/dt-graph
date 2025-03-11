package it.unibo.graph.query

import it.unibo.graph.App
import it.unibo.graph.interfaces.*
import org.jetbrains.kotlinx.dataframe.math.mean
import kotlin.math.max
import kotlin.math.min

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
    val result =
        if (match.size == 1) {
            pattern1
        } else {
            join(pattern1, match, joinFilters, mapAliases, where, from, to, timeaware)
        }
            .flatMap { row -> replaceTemporalProperties(row, match, by, mapAliases, from, to) }
            .groupBy { row -> groupBy(by, mapAliases, row, match) }
            .mapValues { aggregate(by, it, mapAliases) }
            .map { wrapResult(by, it) }
            .flatten()
    return result
}

private fun join(
    pattern1: List<List<ElemP>>,
    match: List<List<Step?>>,
    joinFilters: List<Compare>,
    mapAliases: Map<String, Pair<Int, Int>>,
    where: List<Compare>,
    from: Long,
    to: Long,
    timeaware: Boolean
) =
    pattern1.map { path -> // for each result (i.e., path) of the first pattern
        var discard = false  // whether this path should be discarded because it will not match
        // MATCH (a), (b) WHERE a.name = b.name
        // RESULT of the first pattern, consider two nodes/paths ({name: "Alice"}), ({surname: "Black"})
        val curMatch = match[1].map { step -> // push down the predicates given the values of the first path
            step?.let { // for each step of the second pattern; e.g. try ({name: "Alice"}) first and then ({surname: "Black"})
                val t = joinFilters // let's consider the joining filters; e.g. a.name = b.name
                    .filter { it.a == step.alias || it.b == step.alias } // take only the joining filters referring to the current step
                    .map {
                        val cAlias =
                            if (step.alias != it.a) it.a else it.b // take the alias of the step in the previous pattern; e.g. "a"
                        val props =
                            path[mapAliases[cAlias]!!.second].getProps(name = it.property) // take the property of the corresponding node/edge in the path; e.g. "a.name"
                        if (props.isEmpty()) { // if the node has no such property, then this path can be discarded before the join // TODO, durante la ricerca dei path potrei già scartarlo tra i risultati possibili?
                            discard = true
                            Triple(it.property, it.operator, "foo")
                        } else {
                            Triple(it.property, it.operator, props[0].value) // else, add this as a filter
                        }
                    }
                Step(
                    step.type,
                    it.properties + t,
                    step.alias
                ) // ... and extend the step; e.g., Step("B", alias="b") becomes Step("B", alias="b", property=("name", EQ, "Alice")
            }
        }
        if (discard) { // if the path is discarded, do not perform the search
            emptyList()
        } else { // else, search for valid paths for the second pattern
            search(
                curMatch,
                (where - joinFilters).filter { mapAliases[it.a]!!.first == 1 },
                from,
                to,
                timeaware
            ).map { path + it }
        }
    }
    .flatten()

private fun replaceTemporalProperties(row: List<ElemP>, match: List<List<Step?>>, by: List<Aggregate>, mapAliases: Map<String, Pair<Int, Int>>, from: Long, to: Long): List<List<Any>> {
    // Find the nodes that should be replaced by their properties
    // MATCH (n)-->(m) RETURN n.name, m => only n
    // MATCH (n)-->(m) RETURN n.name, avg(m.value) => n and m
    val toReplace = by.filter { it.property != null }.associateBy {
        val alias = mapAliases[it.n]!!
        alias.second + (if (alias.first == 1) match[0].size else 0)
    } // find its index in the path
    val acc: MutableList<List<Any>> = mutableListOf() // accumulator of rows
    return if (toReplace.isNotEmpty()) { // if I need to replace at least one element
        fun rec(curRow: List<Any>, index: Int, from: Long, to: Long) { // recursive function
            if (index < row.size) { // iterate until all elements have been checked
                val replace = toReplace[index] // find the next item to replace
                if (replace == null) {  // if I don't need to replace this item... continue the recursion but compute the reduced from/to timestamps
                    val elem = row[index]
                    val newFrom = max(from, elem.fromTimestamp)
                    val newTo = min(to, elem.toTimestamp)
                    if (!(newFrom > to || newTo < from)) { // this node/edge is still valid (i.e., it overlaps with the time span of the property)
                        rec(curRow + listOf(row[index]), index + 1, newFrom, newTo) // continue the recursion
                    }
                } else { // else, I need to replace this items
                    val props = row[index].getProps(name = replace.property, fromTimestamp = from, toTimestamp = to) // find the properties to replace
                    if (props.isEmpty()) { // if they are empty, add the null value as a return
                        rec(curRow + listOf("null"), index + 1, from, to) // ... and continue the recursion
                    } else {
                        /* I cannot simply iterate over the properties to start the recursion, for instance let's consider
                         * (a)---[0, 0)--->(b1)
                         * (a)---[1, 1)--->(b2)
                         * (a)---[2, 2)--->(b3)
                         *
                         * where (a) has two properties
                         * a.name = "foo" in (0, 0)
                         * a.name = "bar" in (1, 1)
                         *
                         * The query MATCH (a)-->(b) RETURN a.name, b
                         * must return
                         * -------------
                         * | foo  | b1 |
                         * | bar  | b2 |
                         * | null | b3 |
                         * -------------
                         * So I need to check the time span of the next element to decide whether I should push a "null" value down the recursion
                         */
                        if (index + 1 < row.size) { // if this element is not the last
                            val nextElem = row[index + 1] // look ahead the next element
                            val overlappingProperties = props.filter { p -> !(p.fromTimestamp > nextElem.toTimestamp || p.toTimestamp < nextElem.fromTimestamp) } // find the overlapping properties
                            if (overlappingProperties.isEmpty()) { // if there is no overlapping, push null down to the recursion
                                rec(curRow + listOf("null"), index + 1, from, to)
                            } else {
                                overlappingProperties.forEach { p ->  // if an overlapping property exists
                                    rec(curRow + listOf(p.value), index + 1, max(from, p.fromTimestamp), min(to, p.toTimestamp)) // continue the recursion by further reducing the interval with the property values
                                }
                            }
                        } else { // If this is the last element, simply iterate over the existing properties
                            props.forEach { p ->  // if a property exists
                                rec(curRow + listOf(p.value), index + 1, max(from, p.fromTimestamp), min(to, p.toTimestamp)) // continue the recursion by further reducing the interval with the property values
                            }
                        }
                    }
                }
            } else {
                acc.add(curRow)
            }
        }
        rec(listOf(), 0, from, to)
        acc
    } else {
        listOf(row)
    }
}

private fun wrapResult(by: List<Aggregate>, it: Map.Entry<List<Any>, List<Any>>) =
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

private fun aggregate(by: List<Aggregate>, group: Map.Entry<List<Any>, List<List<Any>>>, mapAliases: Map<String, Pair<Int, Int>>) =
    if (!by.any { it.operator != null }) {
        // no aggregation operator has been specified
        group.value
    } else {
        // some aggregation operator has been specified
        // TODO apply different aggregation operators, and possibly multiple aggregation operators
        val value: Double =
            group.value.map { row -> (row[mapAliases[by.first { it.operator != null }.n]!!.second] as Number).toDouble() }
                .mean(true)
        listOf(value) // E.g., [12.5]
    }

private fun groupBy(by: List<Aggregate>, mapAliases: Map<String, Pair<Int, Int>>, row: List<Any>, match: List<List<Step?>>) =
    // group by all elements that are not aggregation operator
    // MATCH (n)-->(m) RETURN n.name, m.name => group by n and m
    // MATCH (n)-->(m) RETURN n.name, avg(m.value) => group by only on n
    by.filter { it.operator == null }.map {
        val alias = mapAliases[it.n]!!
        row[alias.second + (if (alias.first == 1) match[0].size else 0)]
    }

fun search(match: List<Step?>, where: List<Compare> = listOf(), from: Long = Long.MIN_VALUE, to: Long = Long.MAX_VALUE, timeaware: Boolean = false): List<List<ElemP>> {
    val visited: MutableSet<Number> = mutableSetOf()
    val acc: MutableList<List<ElemP>> = mutableListOf()
    val mapWhere: Map<String, Compare> = where.associateBy { it.b }
    val mapAlias: Map<String, Int> = match.mapIndexed { a, b -> Pair(a, b) }.filter { it.second?.alias != null }.associate { it.second?.alias!! to it.first }

    //fun timeOverlap(it: Elem, from: Long, to: Long): Boolean = !timeaware || !(to < it.fromTimestamp || from >= it.toTimestamp)

    fun dfs(e: ElemP, index: Int, path: List<ElemP>, from: Long, to: Long) {
        val alias: String? = match[index]?.alias
        val c: Compare? = if (alias != null) mapWhere[alias] else null
        if ((match[index] == null || ( // no filter
                (match[index]!!.type == null || match[index]!!.type == e.type)  // filter on label
                && match[index]!!.properties.all { f -> e.getProps(name = f.first, fromTimestamp = from, toTimestamp = to).any { p -> p.value == f.third }})) // filter on properties, TODO should implement different operators
            && e.timeOverlap(timeaware, from, to) // check time overlap
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