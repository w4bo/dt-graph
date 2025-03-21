package it.unibo.graph.query

import it.unibo.graph.interfaces.*
import it.unibo.graph.interfaces.Labels.HasTS
import org.jetbrains.kotlinx.dataframe.math.mean
import kotlin.math.max
import kotlin.math.min

@JvmName("query")
fun query(g: Graph, match: List<Step?>, where: List<Compare> = listOf(), by: List<Aggregate> = listOf(), from: Long = Long.MIN_VALUE, to: Long = Long.MAX_VALUE, timeaware: Boolean = true): List<Any> {
    return query(g, listOf(match), where, by, from, to, timeaware)
}

@JvmName("queryJoin")
fun query(g: Graph, match: List<List<Step?>>, where: List<Compare> = listOf(), by: List<Aggregate> = listOf(), from: Long = Long.MIN_VALUE, to: Long = Long.MAX_VALUE, timeaware: Boolean = true): List<Any> {
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
    val pattern1 = search(g, match[0], (where - joinFilters).filter { mapAliases[it.a]!!.first == 0 }, from, to, timeaware) // compute the first pattern by removing all join filters and consider only the filters on the first pattern (i.e., patternIndex = 0)
    val result =
        if (match.size == 1) {
            pattern1
        } else {
            join(g, pattern1, match, joinFilters, mapAliases, where, from, to, timeaware)
        }
        .flatMap { row -> replaceTemporalProperties(row, match, by, mapAliases, from, to, timeaware) }
        .groupBy { row -> groupBy(by, mapAliases, row, match) }
        .mapValues { aggregate(by, it, mapAliases) }
        .map { wrapResult(by, it) }
        .flatten()
    return result
}

/**
 * Join two patterns
 */
private fun join(g: Graph, pattern1: List<Path>, match: List<List<Step?>>, joinFilters: List<Compare>, mapAliases: Map<String, Pair<Int, Int>>, where: List<Compare>, from: Long, to: Long, timeaware: Boolean) =
    pattern1.flatMap { row -> // for each result (i.e., path) of the first pattern
        // Restrict the temporal interval
        val from = max(from, row.from)
        val to = min(to, row.to)
        val row = row.result
        val acc: MutableList<Path> = mutableListOf()
        fun rec(curMatch: List<Step?>, index: Int, from: Long, to: Long) {
            if (index < match[1].size) { // Iterate until the last step of match[1] (i.e., the second pattern)
                val step = match[1][index] // Get the current step
                if (step != null) { // If the step has some filters
                    if (joinFilters.size > 1) throw IllegalArgumentException("Too many joining filters")
                    val curJoinFilters = joinFilters.filter { step.alias == it.a || step.alias == it.b } // Check if the step contains a node to join
                    if (curJoinFilters.isNotEmpty()) { // ... if so
                        if (curJoinFilters.size > 1) throw IllegalArgumentException("Too many joining filters on a single Step")
                        val curJoinFilter = curJoinFilters.first() // Take the filter condition. TODO I assume that there is a single joining filter on a step
                        val cAlias = if (step.alias != curJoinFilter.a) curJoinFilter.a else curJoinFilter.b // take the alias of the step in the previous pattern; e.g. "a"
                        val props = row[mapAliases[cAlias]!!.second].getProps(name = curJoinFilter.property, fromTimestamp = from, toTimestamp = to, timeaware = timeaware) // take the properties of the corresponding node/edge in the path; e.g. "a.name" and its historic versions
                        props.forEach { p -> // explode each property
                            rec(curMatch + Step(step.type, step.properties + Triple(curJoinFilter.property, curJoinFilter.operator, p.value), step.alias), index + 1, max(from, p.fromTimestamp), min(to, p.toTimestamp)) // else, add this as a filter
                        }
                    } else {
                        rec(curMatch + step, index + 1, from, to)
                    }
                } else {
                    rec(curMatch + step, index + 1, from, to)
                }
            } else {
                acc += search(g, curMatch, (where - joinFilters).filter { mapAliases[it.a]!!.first == 1 }, from, to, timeaware).map { Path(row + it.result, it.from, it.to) }
            }
        }
        rec(listOf(), 0, from, to)
        acc
    }

/**
 * Replace properties if necessary
 */
private fun replaceTemporalProperties(row: Path, match: List<List<Step?>>, by: List<Aggregate>, mapAliases: Map<String, Pair<Int, Int>>, from: Long, to: Long, timeaware: Boolean): List<List<Any>> {
    // Restrict the temporal interval
    val from = max(from, row.from)
    val to = min(to, row.to)
    val row = row.result
    // Find the nodes that should be replaced by their properties
    // MATCH (n)-->(m) RETURN n.name, m => only n
    // MATCH (n)-->(m) RETURN n.name, avg(m.value) => n and m
    val toReplace = by.filter { it.property != null }.associateBy {
        val alias = mapAliases[it.n] ?: throw IllegalArgumentException("Alias `${it.n}` is not defined")
        alias.second + (if (alias.first == 1) match[0].size else 0)
    } // find its index in the path
    val acc: MutableList<List<Any>> = mutableListOf() // accumulator of rows
    return if (toReplace.isNotEmpty()) { // if I need to replace at least one element
        fun rec(curRow: List<Any>, index: Int, from: Long, to: Long) { // recursive function
            if (index < row.size) { // iterate until all elements have been checked
                val replace = toReplace[index] // find the next item to replace
                if (replace == null) {  // if I don't need to replace this item... continue the recursion but compute the reduced from/to timestamps
                    val elem = row[index]
                    if (timeOverlap(from, to, elem.fromTimestamp, elem.toTimestamp, timeaware)) { // this node/edge is still valid (i.e., it overlaps with the time span of the property)
                        val newFrom = max(from, elem.fromTimestamp)
                        val newTo = min(to, elem.toTimestamp)
                        rec(curRow + row[index], index + 1, newFrom, newTo) // continue the recursion
                    }
                } else { // else, I need to replace these items
                    val props = row[index].getProps(name = replace.property, fromTimestamp = from, toTimestamp = to, timeaware = timeaware) // find the properties to replace
                    if (props.isEmpty()) { // if they are empty, add the null value as a return
                        rec(curRow + "null", index + 1, from, to) // ... and continue the recursion
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
                                rec(curRow + "null", index + 1, from, to)
                            } else {
                                overlappingProperties.forEach { p ->  // if an overlapping property exists
                                    rec(curRow + p.value, index + 1, max(from, p.fromTimestamp), min(to, p.toTimestamp)) // continue the recursion by further reducing the interval with the property values
                                }
                            }
                        } else { // If this is the last element, simply iterate over the existing properties
                            props.forEach { p ->  // if a property exists
                                rec(curRow + p.value, index + 1, max(from, p.fromTimestamp), min(to, p.toTimestamp)) // continue the recursion by further reducing the interval with the property values
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

/**
 * Make the result compatible with Neo4J
 */
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

/**
 * Aggregate the result of group by
 */
private fun aggregate(by: List<Aggregate>, group: Map.Entry<List<Any>, List<List<Any>>>, mapAliases: Map<String, Pair<Int, Int>>) =
    if (!by.any { it.operator != null }) {
        // no aggregation operator has been specified
        // E.g., MATCH (n) RETURN n.name => [a, b, ...]
        group.value
    } else {
        // some aggregation operator has been specified
        // E.g., MATCH (n)-->(m) RETURN n.name, avg(m.value) => [[a, 12.5], [b, 13.0], ...]
        // TODO apply different aggregation operators, and possibly multiple aggregation operators
        val value: Double = group.value.map { row -> (row[mapAliases[by.first { it.operator != null }.n]!!.second] as Number).toDouble() }.mean(true)
        listOf(value) // E.g., [12.5]
    }

/**
 * Group several rows by key
 */
private fun groupBy(by: List<Aggregate>, mapAliases: Map<String, Pair<Int, Int>>, row: List<Any>, match: List<List<Step?>>) =
    // group by all elements that are not aggregation operator
    // MATCH (n)-->(m) RETURN n.name, m.name => group by n and m
    // MATCH (n)-->(m) RETURN n.name, avg(m.value) => group by only on n
    by.filter { it.operator == null }.map {
        val alias = mapAliases[it.n]!!
        row[alias.second + (if (alias.first == 1) match[0].size else 0)]
    }

fun search(g: Graph, match: List<Step?>, where: List<Compare> = listOf(), from: Long = Long.MIN_VALUE, to: Long = Long.MAX_VALUE, timeaware: Boolean = false): List<Path> {
    val acc: MutableList<Path> = mutableListOf()
    val mapAlias: Map<String, Int> = match.mapIndexed { a, b -> Pair(a, b) }.filter { it.second?.alias != null }.associate { it.second?.alias!! to it.first }
    val mapWhere: Map<String, Compare> = where.associateBy{mapAlias.filterKeys{it in where.flatMap { listOf(it.a, it.b) }.toSet()}.maxByOrNull { it.value }?.key!!}

    fun dfs(e: ElemP, index: Int, path: List<ElemP>, from: Long, to: Long, visited: Set<Number>) {
        fun whereClause(
            e: ElemP,
            path: List<ElemP>,
            alias: String,
            mapWhere: Map<String, Compare>,
            c: Compare,
            timeaware: Boolean
        ): Boolean {
            if (mapWhere[alias]?.a == alias) {
                return c.isOk(e, path[mapAlias[c.b]!!], timeaware)
            } else if(mapWhere[alias]?.b == alias){
                return c.isOk(path[mapAlias[c.a]!!], e, timeaware)
            }else{
                return false
            }
        }
        val alias: String? = match[index]?.alias
        val c: Compare? = if (alias != null) mapWhere[alias] else null
        if ((match[index] == null || ( // no filter
                    (match[index]!!.type == null || match[index]!!.type == e.label)  // filter on label
                            && match[index]!!.properties.all { f ->
                        e.getProps(
                            name = f.first,
                            fromTimestamp = from,
                            toTimestamp = to,
                            timeaware = timeaware
                        ).any { p -> Compare.apply(f.third, p.value, f.second) }
                    })) // filter on properties
            && e.timeOverlap(timeaware, from, to) // check time overlap
            && (c == null || whereClause(e, path, alias!!, mapWhere, c, timeaware)) // apply the where clause
        ) {
            val curPath = path + listOf(e)
            if (curPath.size == match.size) {
                acc.add(Path(curPath, from, to))
            } else {
//                if (visited.contains(e.id)) {
//                    return
//                }
                val from = max(e.fromTimestamp, from)
                val to = min(e.toTimestamp, to)
                if (index % 2 == 0) { // is node
                    (e as N)
                        .getRels(direction = Direction.OUT, includeHasTs = true)
                        .forEach {
                            dfs(it, index + 1, curPath, from, to, visited + e.id)
                        }
                } else { // is edge...
                    val r = (e as R)
                    if (e.label == HasTS) { // ... to time series
                        g.getTSM()
                            .getTS(r.toN)
                            .getValues()
                            .forEach {
                                dfs(it, index + 1, curPath, from, to, visited)
                            }
                    } else { // ... or to graph node
                        val n = g.getNode(r.toN)
                        dfs(n, index + 1, curPath, from, to, visited)
                    }
                }
            }
        }
    }
    for (node in g.getNodes()) {
        dfs(node, 0, emptyList(), from, to, mutableSetOf())
    }
    return acc
}