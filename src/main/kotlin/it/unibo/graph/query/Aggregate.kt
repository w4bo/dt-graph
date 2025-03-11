package it.unibo.graph.query

enum class AggOperator { SUM, COUNT, AVG, MIN, MAX }

class Aggregate(val n: String, val property: String? = null, val operator: AggOperator? = null)