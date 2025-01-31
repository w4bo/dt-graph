package it.unibo.graph.structure

import it.unibo.graph.PropType
import org.apache.commons.configuration2.Configuration
import org.apache.commons.lang3.NotImplementedException
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.Transaction
import org.apache.tinkerpop.gremlin.structure.Vertex

class CustomGraph : Graph {
    override fun addVertex(vararg keyValues: Any?): Vertex {
        val n = it.unibo.graph.Graph.addNode2(keyValues[1].toString(), this) as CustomVertex
        keyValues.toList().forEachIndexed { idx, v -> if (idx % 2 == 0) {
            it.unibo.graph.Graph.addProperty2(n.id, v!!.toString(), keyValues[idx + 1]!!, PropType.STRING)}
        }
        return n
    }

    override fun <C : GraphComputer?> compute(graphComputerClass: Class<C>?): C {
        throw NotImplementedException()
    }

    override fun compute(): GraphComputer {
        throw NotImplementedException()
    }

    override fun vertices(vararg vertexIds: Any?): Iterator<Vertex> {
        return it.unibo.graph.Graph.nodes.map { it as CustomVertex }.iterator()
    }

    override fun edges(vararg edgeIds: Any?): Iterator<Edge> {
        return it.unibo.graph.Graph.rels.map { it as CustomEdge }.iterator()
    }

    override fun tx(): Transaction {
        throw NotImplementedException()
    }

    override fun variables(): Graph.Variables {
        throw NotImplementedException()
    }

    override fun configuration(): Configuration {
        throw NotImplementedException()
    }

    override fun close() {
        return it.unibo.graph.Graph.clear()
    }
}
