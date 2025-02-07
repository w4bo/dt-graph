package it.unibo.graph

import it.unibo.graph.structure.CustomVertex
import java.io.Serializable

class Statistic(
    val offset: Int,
    var maxTime: Long = Long.MIN_VALUE,     // Largest timestamp in the time series
    var minValue: Long = Long.MAX_VALUE,     // Smallest value in the time series.
    var maxValue: Long = Long.MIN_VALUE,     // Largest value in the time series.
    var mbr_x_left: Double = Double.POSITIVE_INFINITY,     // Left boundary of MBR (minimum x)
    var mbr_y_left: Double = Double.POSITIVE_INFINITY, // Bottom boundary of MBR (minimum y)
    var mbr_x_right: Double = Double.NEGATIVE_INFINITY,  // Right boundary of MBR (maximum x)
    var mbr_y_right: Double = Double.NEGATIVE_INFINITY,  // Top boundary of MBR (maximum y)
    var count: Long = 0  // Counter for the number of elements in the time series.
) {
    fun updateTime(timestamp: Long) {
        // Update the maximum timestamps.
        maxTime = timestamp
    }

    fun updateValue(value: Number) {
        // Update the minimum and maximum values.
        minValue = minValue.coerceAtMost((value).toLong())
        maxValue = maxValue.coerceAtLeast((value).toLong())
    }

    fun updateLocation(location: Pair<Double, Double>) {
        mbr_x_left = mbr_x_left.coerceAtMost(location.first)  // Update left boundary.
        mbr_y_left = mbr_y_left.coerceAtMost(location.second) // Update bottom boundary.
        mbr_x_right = mbr_x_right.coerceAtLeast(location.first)  // Update right boundary.
        mbr_y_right = mbr_y_right.coerceAtLeast(location.second) // Update top boundary.
    }
}

interface TS: Serializable {
    fun getTSID(): Long
    fun nextId(): Int
    fun add(label:String, timestamp: Long, value: Long) = add(N(nextId(), label, timestamp=timestamp, value=value))
    fun add(n: N): N
    fun getValues(): List<N>
}

class MemoryTS(val id: Int) : TS {
    private val values: MutableList<N> = mutableListOf()

    override fun getTSID(): Long = id.toLong()

    override fun nextId(): Int = values.size

    override fun add(n: N): N {
        values += n
        return n
    }

    override fun getValues(): List<N> = values
}

class CustomTS(ts: TS): TS by ts {
    override fun add(label: String, timestamp: Long, value: Long): N {
        return add(CustomVertex(nextId(), label, timestamp = timestamp, value = value))
    }
}

class FooTS(val id: Int, val values: MutableList<N> = mutableListOf()) {

    var sparseIndex: List<Pair<Long, Statistic>> = mutableListOf()
    var curStatistic = Statistic(0)
    val allStatistic = Statistic(0)

    var elements = 0
    var limit = 2

    fun add(ts: N) {
        elements = (elements + 1) % limit
        if (elements == 0) {
            curStatistic = Statistic(values.size)
            sparseIndex += Pair(ts.timestamp!!, curStatistic)
        }
        // Add the new element to the list of values.
        values += ts

        // update local statistics
        curStatistic.updateTime(ts.timestamp!!)
        curStatistic.updateValue(ts.value!!)
        curStatistic.count++
        if (ts.location != null) {
            curStatistic.updateLocation(ts.location)
        }

        // update global statistics
        allStatistic.updateTime(ts.timestamp)
        allStatistic.updateValue(ts.value)
        allStatistic.count++
        if (ts.location != null) {
            allStatistic.updateLocation(ts.location)
        }
    }
}
