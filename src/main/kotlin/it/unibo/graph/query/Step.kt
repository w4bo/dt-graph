package it.unibo.graph.query

import it.unibo.graph.interfaces.Label

interface IStep {
    val type: Label?
    val properties: List<Filter>
    val alias: String?
}

class Step(
    override val type: Label? = null,
    override val properties: List<Filter> = emptyList(),
    override val alias: String? = null
) : IStep
