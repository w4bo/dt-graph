package it.unibo.graph.query

import it.unibo.graph.interfaces.ElemP
import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.geojson.GeoJsonReader

enum class Operators { EQ, LT, GT, LTE, GTE, ST_CONTAINS }

class Compare(val a: Any, val b: Any, val property: String, val operator: Operators) {

    companion object {
        fun apply(a: Any, b: Any, operator: Operators): Boolean {
            return Compare(a, b, "", operator).compareIfSameType(a, b, operator)
        }
    }
    private fun compareIfSameType(a: Any, b: Any, operator: Operators): Boolean {

        if (operator == Operators.EQ) return a == b

        if (a is Comparable<*> && b is Comparable<*>) {
            @Suppress("UNCHECKED_CAST")
            val compA = a as Comparable<Any>
            val compB = b as Any

            return when (operator) {
                Operators.LT -> compA < compB
                Operators.GT -> compA > compB
                Operators.LTE -> compA <= compB
                Operators.GTE -> compA >= compB
                Operators.ST_CONTAINS -> geometryContains(a, b)
                else -> false
            }
        }
        return false
    }

    private fun geometryContains(a: Any, b: Any): Boolean {
        val parser = GeoJsonReader()
        if (a == "" || b == "") {
            return false
        }
        val geomA = when (a) {
            is Geometry -> a
            is String -> parser.read(a)
            else -> throw IllegalArgumentException("Invalid type for 'a': ${a::class.simpleName}")
        }
        val geomB = when (b) {
            is Geometry -> b
            is String -> parser.read(b)
            else -> throw IllegalArgumentException("Invalid type for 'b': ${b::class.simpleName}")
        }
        return geomA.contains(geomB)
    }

    fun isOk(a: ElemP, b: ElemP, timeaware: Boolean): Boolean {
        val p1 = a.getProps(name = property)
        val p2 = b.getProps(name = property)
        return p1.isNotEmpty() && p2.isNotEmpty() && compareIfSameType(p1[0].value, p2[0].value, operator) && a.timeOverlap(timeaware, b.fromTimestamp, b.toTimestamp)
    }
}