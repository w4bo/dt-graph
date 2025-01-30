package it.unibo.graph

import it.unibo.graph.structure.CustomEdge
import it.unibo.graph.structure.CustomProperty
import it.unibo.graph.structure.CustomVertex

object Graph {
    val nodes: MutableList<N> = ArrayList()
    val rels: MutableList<R> = ArrayList()
    val props: MutableList<P> = ArrayList()
    val ts: MutableList<TS> = ArrayList()

    fun clear() {
        nodes.clear()
        rels.clear()
        props.clear()
        ts.clear()
    }

    fun search(labels: List<String>): MutableList<List<N>> {
        var visited: Set<Int> = mutableSetOf()
        var acc: MutableList<List<N>> = mutableListOf()

        fun dfs(node: N, index: Int, path: List<N>) {
            if (node.type == labels[index]) {
                val cur_path = path + listOf(node)
                if (cur_path.size == labels.size) {
                    acc.add(cur_path)
                    return
                }
                if (visited.contains(node.id)) { return }
                visited += node.id
                node.getRels(direction = Direction.OUT).forEach {
                    dfs(nodes[it.toN], index + 1, cur_path)
                }
            }
        }

        for (node in nodes) {
            if (!visited.contains(node.id)) {
                dfs(node, 0, listOf())
            }
        }
        return acc
    }

    fun addNode(label: String): N {
        val n = N(nodes.size, label)
        nodes += n
        return n
    }

    fun addProperty(nodeId: Int, key: String, value: Any, type: PropType): P {
        val p = P(props.size, nodeId, key, value, type)
        props += p
        return p
    }

    fun addRel(label: String, fromNode: Int, toNode: Int): R {
        val r = R(rels.size, label, fromNode, toNode)
        rels += r
        return r
    }

    fun addNode2(label: String, graph: org.apache.tinkerpop.gremlin.structure.Graph): N {
        val n = CustomVertex(nodes.size, label, graph)
        nodes += n
        return n
    }

    fun addProperty2(nodeId: Int, key: String, value: Any, type: PropType): P {
        val p = CustomProperty<String>(props.size, nodeId, key, value, type)
        props += p
        return p
    }

    fun addRel2(label: String, fromNode: Int, toNode: Int, graph: org.apache.tinkerpop.gremlin.structure.Graph): R {
        val r = CustomEdge(rels.size, label, fromNode, toNode, graph)
        rels += r
        return r
    }

    @JvmStatic
    fun main(args: Array<String>) {

    }
}