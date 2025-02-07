package it.unibo.graph.structure

import it.unibo.graph.*
import org.apache.commons.configuration2.Configuration
import org.apache.commons.lang3.NotImplementedException
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.Transaction
import org.apache.tinkerpop.gremlin.structure.Vertex

class CustomGraph(g: it.unibo.graph.Graph): Graph, it.unibo.graph.Graph by g  {
    override fun addVertex(vararg keyValues: Any?): Vertex {
        val n = CustomVertex(nextNodeId(), keyValues[1].toString(), this, value=null)
        addNode(n)
        keyValues
            .toList()
            .forEachIndexed { idx, v -> if (idx % 2 == 0) {
                addProperty(n.id, v!!.toString(), keyValues[idx + 1]!!, PropType.STRING)}
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
        clear()
    }

    override fun addNode(label: String, value: Long?): N {
        return addNode(CustomVertex(getNodes().size, label, this, value=value))
    }

    override fun addProperty(nodeId: Int, key: String, value: Any, type: PropType): P {
        return addProperty(CustomProperty<String>(getProps().size, nodeId, key, value, type))
    }

    override fun addEdge(label: String, fromNode: Int, toNode: Int): R {
        return addEdge(CustomEdge(getEdges().size, label, fromNode, toNode, this))
    }
}
