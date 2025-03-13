package it.unibo.graph.structure

import it.unibo.graph.interfaces.N
import it.unibo.graph.interfaces.P
import it.unibo.graph.interfaces.PropType
import it.unibo.graph.interfaces.R
import org.apache.commons.configuration2.Configuration
import org.apache.commons.lang3.NotImplementedException
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.Transaction
import org.apache.tinkerpop.gremlin.structure.Vertex

class CustomGraph(val g: it.unibo.graph.interfaces.Graph) : Graph, it.unibo.graph.interfaces.Graph by g {
    override fun addVertex(vararg keyValues: Any?): Vertex {
        val n = this.addNode(keyValues[1].toString()) as CustomVertex
        keyValues
            .toList()
            .forEachIndexed { idx, v ->
                if (idx % 2 == 0) {
                    addProperty(n.id, v!!.toString(), keyValues[idx + 1]!!, PropType.STRING)
                }
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
        return getNodes().map { it as CustomVertex }.iterator()
    }

    override fun edges(vararg edgeIds: Any?): Iterator<Edge> {
        return getEdges().map { it as CustomEdge }.iterator()
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
        g.close()
        clear()
    }

    override fun addNode(label: String, value: Long?, from: Long, to: Long): N {
        return addNode(CustomVertex(nextNodeIdOffset(), label, value = value, fromTimestamp = from, toTimestamp = to, g = g))
    }

    override fun addProperty(sourceId: Long, key: String, value: Any, type: PropType, from: Long, to: Long, sourceType: Boolean, id: Int): P {
        return addProperty(
            CustomProperty<String>(id, sourceId, sourceType, key, value, type, from, to, g = g)
        )
    }

    override fun addEdge(label: String, fromNode: Long, toNode: Long, id: Int, from: Long, to: Long): R {
        return addEdge(CustomEdge(id, label, fromNode, toNode, fromTimestamp = from, toTimestamp = to, g = g))
    }
}
