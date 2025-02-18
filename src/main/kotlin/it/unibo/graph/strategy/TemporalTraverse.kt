package it.unibo.graph.strategy

import org.apache.tinkerpop.gremlin.process.traversal.Traversal
import org.apache.tinkerpop.gremlin.process.traversal.Traverser
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Vertex


class FilterEdge(previousStep: AbstractStep<*, *>) : AbstractStep<Vertex, Vertex>(previousStep.getTraversal<Any, Any>()) {
    private var previousEdge: Edge? = null

    override fun remove() {
        TODO("Not yet implemented")
    }

    override fun processNextStart(): Traverser.Admin<Vertex>? {
//        val traverser = previousStep.next() // starts.next()
//        val currentElement: Vertex = traverser.get()
//        if (previousEdge == null) {
//            previousEdge = currentElement
//            return this.starts.next()
//        } else {
//            if ((previousEdge!!.property<Long>("from") as Long <= currentElement.property<Long>("from") as Long) &&
//                (previousEdge!!.property<Long>("to") as Long >= currentElement.property<Long>("to") as Long)
//            ) {
//                previousEdge = currentElement;
//                return traverser;
//            }
//        }
        return null
    }
}