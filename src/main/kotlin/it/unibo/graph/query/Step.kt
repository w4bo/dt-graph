package it.unibo.graph.query

import it.unibo.graph.interfaces.Direction
<<<<<<< HEAD
import it.unibo.graph.interfaces.Label

interface IStep {
    val type: Label?
=======

interface IStep {
    val label: String?
>>>>>>> feat-tssingletable
    val properties: List<Filter>
    val alias: String?
}

open class Step(
<<<<<<< HEAD
    override val type: Label? = null,
=======
    override val label: String? = null,
>>>>>>> feat-tssingletable
    override val properties: List<Filter> = emptyList(),
    override val alias: String? = null
) : IStep

class EdgeStep(
<<<<<<< HEAD
    type: Label? = null,
=======
    label: String? = null,
>>>>>>> feat-tssingletable
    properties: List<Filter> = emptyList(),
    alias: String? = null,
    val direction: Direction
) :
<<<<<<< HEAD
    Step(type, properties, alias)
=======
    Step(label, properties, alias)
>>>>>>> feat-tssingletable
