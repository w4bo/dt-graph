package it.unibo.graph.strategy


import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy.FinalizationStrategy
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper
import org.apache.tinkerpop.gremlin.structure.Edge


class TrackEdgeWeightStrategy : AbstractTraversalStrategy<FinalizationStrategy?>(), FinalizationStrategy {
    override fun apply(traversal: Traversal.Admin<*, *>) {
        val stepsToModify: MutableList<Pair<Int, AbstractStep<*, *>>> = ArrayList()

        // Collect steps without modifying the traversal while iterating
        traversal.steps.forEachIndexed() { index, step ->
            if (step is VertexStep /* && step.labels.contains("workedAt") */) {
                stepsToModify.add(Pair(index, step))
            }
        }

        // Modify the traversal **after** collecting target steps
        var c: Int = 1
        for ((index, step) in stepsToModify) {
            traversal.addStep<Any, Any>(index + c++, FilterEdge(step))
        }
    }
}
