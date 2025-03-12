package it.unibo.graph.utils

import it.unibo.graph.interfaces.Elem
import it.unibo.graph.interfaces.Graph
import java.io.*

// Serialize an object to byte array
fun serialize(obj: Serializable): ByteArray {
    ByteArrayOutputStream().use { bos ->
        ObjectOutputStream(bos).use { out -> out.writeObject(obj) }
        return bos.toByteArray()
    }
}

// Deserialize a byte array to an object
inline fun <reified T : Elem> deserialize(bytes: ByteArray?, g: Graph): T {
    val r: T = ByteArrayInputStream(bytes).use { bis ->
        ObjectInputStream(bis).use { `in` -> `in`.readObject() as T }
    }
    (r as Elem).g = g
    return r
}

fun encodeBitwise(x: Long, y: Long, offset: Int = 44, mask: Long = 0xFFFFFFFFFFF): Long {
    return (x shl offset) or (y and mask)
}

fun decodeBitwise(z: Long, offset: Int = 44, mask: Long = 0xFFFFFFFFFFF): Pair<Long, Long> {
    val x = (z shr offset)
    val y = (z and mask)
    return Pair(x, y)
}

fun decodeBitwiseSource(z: Long, offset: Int = 44): Long {
    return z shr offset
}