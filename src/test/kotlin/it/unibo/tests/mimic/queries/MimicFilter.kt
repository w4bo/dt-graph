package it.unibo.tests.mimic.queries

import it.unibo.graph.interfaces.Graph
import it.unibo.graph.query.Filter
import it.unibo.graph.query.Operators
import it.unibo.graph.query.QueryMode
import it.unibo.graph.query.Step
import it.unibo.graph.utils.Person
import it.unibo.graph.utils.VALUE
import it.unibo.tests.mimic.TestMimicQuery.Q
import it.unibo.tests.mimic.mimicFilter

class MimicFilter(val graph: Graph, val pId: Long, c: Long) : Q() {
    override val queryId: String = "MimicFilter_${pId}_$c"
    override fun query(threads: Int, queryMode: QueryMode): List<Any> {
        return it.unibo.graph.query.query(
            graph,
            listOf(
                Step(Person, properties = listOf(Filter("subject_id", Operators.EQ, pId.toInt()))),
                null,
                Step(label = "HR"),
                null,
                Step(properties = listOf(Filter(VALUE, Operators.GT, mimicFilter)))
            ),
            threads = threads,
            mode = queryMode
        )
    }
}