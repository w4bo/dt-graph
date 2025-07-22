package it.unibo.graph.inmemory

import it.unibo.graph.interfaces.*
import it.unibo.graph.utils.DUMMY_ID
import it.unibo.graph.utils.serialize
import java.io.File
import java.io.IOException
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util.concurrent.locks.ReentrantLock

const val PATH = "db_graphm"

interface Flushable {
    fun flushToDisk()
}

class MemoryGraphACID(
    val wal: WriteAheadLog = WriteAheadLog(),
    path: String = PATH,
    nodeFile: String = "nodes.dat",
    edgeFile: String = "edges.dat",
    propertyFile: String = "property.dat",
    nodes: MutableList<N> = ArrayList(),
    rels: MutableList<R> = ArrayList(),
    props: MutableList<P> = ArrayList()
) : MemoryGraph(nodes, rels, props), Flushable {
    private val nodeChannel: FileChannel = FileChannel.open(File("$path/$nodeFile").toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
    private val edgeChannel: FileChannel = FileChannel.open(File("$path/$edgeFile").toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
    private val propertyChannel: FileChannel = FileChannel.open(File("$path/$propertyFile").toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE)
    // private val scheduler: ScheduledExecutorService = Executors.newScheduledThreadPool(1)

    init {
        if (!Files.exists(Paths.get(path))) {
            Files.createDirectories(Paths.get(path))
        }
        wal.setFlushable(this)
        // startPeriodicFlush()
    }

    // Generic function to read from disk and deserialize
    private fun <T> getFromDisk(channel: FileChannel, id: Long, size: Int, fromByteArray: (ByteArray, Any) -> T): T {
        val buffer = ByteBuffer.allocate(size) // Allocate buffer of appropriate size
        channel.read(buffer, size * id) // Read up to n bytes
        return fromByteArray(buffer.array(), this) // Deserialize using the provided method
    }

    // Wrapper functions for specific types
    fun getNodeFromDisk(id: Long): N {
        return getFromDisk(nodeChannel, id, NODE_SIZE) { bytes, _ -> N.fromByteArray(bytes, this) }
    }

    fun getEdgeFromDisk(id: Long): R {
        return getFromDisk(edgeChannel, id, EDGE_SIZE) { bytes, _ -> R.fromByteArray(bytes, this) }
    }

    fun getPropertyFromDisk(id: Long): P {
        return getFromDisk(propertyChannel, id, PROPERTY_SIZE) { bytes, _ -> P.fromByteArray(bytes, this) }
    }

    override fun clear() {
        super.clear()
        listOf(nodeChannel, edgeChannel, propertyChannel).forEach {
            it.truncate(0)
            it.position(0)
        }
        wal.clear()
        // scheduler.shutdownNow()
    }

    companion object {
        fun <T> readObjectsFromFile(filePath: String, objectSize: Int, deserializer: (ByteArray) -> T, result: MutableList<T>, offset: Long = 0) {
            RandomAccessFile(filePath, "r").use { file ->
                val channel: FileChannel = file.channel
                channel.position(offset)
                val buffer = ByteArray(objectSize)
                while (true) {
                    val bytesRead = channel.read(java.nio.ByteBuffer.wrap(buffer))
                    if (bytesRead < objectSize) break
                    result.add(deserializer(buffer.copyOf())) // Defensive copy
                }
            }
        }

        fun readFromDisk(path: String = PATH, nodeFile: String = "nodes.dat", edgeFile: String = "edges.dat", propertyFile: String = "property.dat"): MemoryGraphACID {
            val nodes = mutableListOf<N>()
            val rels = mutableListOf<R>()
            val props = mutableListOf<P>()
            val g = MemoryGraphACID(nodes = nodes, rels = rels, props = props)
            readObjectsFromFile("$path/$nodeFile", objectSize = NODE_SIZE, deserializer = { array -> N.fromByteArray(array, g) }, result = nodes)
            readObjectsFromFile("$path/$edgeFile", objectSize = EDGE_SIZE, deserializer = { array -> R.fromByteArray(array, g) }, result = rels)
            readObjectsFromFile("$path/$propertyFile", objectSize = PROPERTY_SIZE, deserializer = { array -> P.fromByteArray(array, g) }, result = props)
            return g
        }
    }

    @Throws(IOException::class)
    override fun flushToDisk() {
        synchronized(wal.toWrite) { // Lock the list to avoid concurrent modifications
            val iterator = wal.toWrite.iterator() // Create an iterator for the list
            while (iterator.hasNext()) {
                val it = iterator.next()
                if (it.flushedOnLog) {
                    // Access file channel based on the source type
                    val file: FileChannel = when (it.file) {
                        WALSource.Nodes -> nodeChannel
                        WALSource.Edges -> edgeChannel
                        else -> propertyChannel
                    }
                    // Write data to the correct file channel at the specified offset
                    file.position(it.offset)
                    file.write(ByteBuffer.wrap(it.payload))
                    // After processing the item, remove it from the list
                    iterator.remove()
                }
            }
        }
    }

    // private fun startPeriodicFlush() {
    //     scheduler.scheduleAtFixedRate({
    //         try {
    //             flushToDisk()
    //         } catch (e: IOException) {
    //             e.printStackTrace()
    //         }
    //     }, 10, 10, TimeUnit.SECONDS) // Flush every 10 seconds
    // }

    @Throws(IOException::class)
    override fun addEdge(r: R): R {
        if (r.id != DUMMY_ID) { // if this is an edge belonging to the graph (and not to a TS)...
            wal.log(WALSource.Edges, r.id.toLong() * EDGE_SIZE, r.serialize())
        }
        return super.addEdge(r)
    }

    @Throws(IOException::class)
    override fun addNode(n: N): N {
        wal.log(WALSource.Nodes, n.id * NODE_SIZE, n.serialize())
        return super.addNode(n)
    }

    @Throws(IOException::class)
    override fun addPropertyLocal(key: Long, p: P): P {
        wal.log(WALSource.Properties, p.id.toLong() * PROPERTY_SIZE, p.serialize())
        return super.addPropertyLocal(key, p)
    }
}

enum class WALSource { Nodes, Edges, Properties }

class WALRecord(val file: WALSource, val offset: Long, val payload: ByteArray, var flushedOnLog: Boolean) :
    java.io.Serializable {
    init {
        if (offset < 0) {
            throw IllegalArgumentException("Offset must be greater than zero.")
        }
    }
}

class WriteAheadLog(fileName: String = "wal.log", path: String = PATH, val frequency: Int = 100) {
    private val logChannel: FileChannel
    private val lock = ReentrantLock()
    val toWrite: MutableList<WALRecord> = mutableListOf()
    private var flushable: Flushable? = null
    private var i = 0
    init {
        if (!Files.exists(Paths.get(path))) {
            Files.createDirectories(Paths.get(path))
        }
        logChannel = FileChannel.open(File("$PATH/$fileName").toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.APPEND)
    }

    @Throws(IOException::class)
    fun log(fileChannel: WALSource, offset: Long, payload: ByteArray) {
        // lock.lock()
        try {
            val record = WALRecord(fileChannel, offset, payload, true)
            toWrite.add(record)
            val buffer = ByteBuffer.wrap(serialize(record))
            logChannel.write(buffer)
            logChannel.force(true)
            if (frequency > 0 && flushable != null) {
                i = (i + 1) % frequency
                if (i == 0) {
                    flushable!!.flushToDisk()
                }
            }
        } finally {
            // lock.unlock()
        }
    }

    fun setFlushable(memoryGraphACID: Flushable) {
        flushable = memoryGraphACID
    }

    fun clear() {
        i = 0
        toWrite.clear()
        logChannel.truncate(0)
        logChannel.position(0)
    }
}
