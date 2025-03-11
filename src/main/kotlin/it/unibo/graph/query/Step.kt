package it.unibo.graph.query

interface IStep {
    val type: String?
    val properties: List<Triple<String, Operators, Any>>
    val alias: String?
}

class Step(
    override val type: String? = null,
    override val properties: List<Triple<String, Operators, Any>> = listOf(),
    override val alias: String? = null
) : IStep
