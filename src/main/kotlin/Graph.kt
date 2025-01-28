object Graph {
    val nodes: MutableList<N> = ArrayList()
    val rels: MutableList<R> = ArrayList()
    val props: MutableList<P> = ArrayList()
    val ts: MutableList<TS> = ArrayList()

    fun search(labels: List<String>): MutableList<List<N>> {
        var visited: Set<Int> = mutableSetOf()
        var acc: MutableList<List<N>> = mutableListOf()

        fun dfs(node: N, index: Int, path: List<N>) {
            visited += node.id
            val cur_path = path + listOf(node)
            if (cur_path.size == labels.size) {
                acc.add(cur_path.subList(cur_path.size - labels.size, cur_path.size))
            } else if (node.type == labels[index]) {
                node.getRels().forEach {
                    if (!visited.contains(it.nextNode)) dfs(Graph.nodes[it.nextNode], index + 1, cur_path)
                }
            }
        }

        for (node in Graph.nodes) {
            if (!visited.contains(node.id)) {
                visited += node.id // set the node as visited
                if (node.type == labels[0]) dfs(node, 0, listOf())
            }
        }
        return acc
    }

    @JvmStatic
    fun main(args: Array<String>) {

    }
}