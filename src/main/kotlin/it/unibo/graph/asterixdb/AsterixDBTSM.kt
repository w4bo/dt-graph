package it.unibo.graph.asterixdb

import it.unibo.graph.interfaces.Graph
import it.unibo.graph.interfaces.TS
import it.unibo.graph.interfaces.TSManager
import it.unibo.graph.utils.incPort
import it.unibo.graph.utils.loadProps
import java.io.OutputStream
import java.io.PrintWriter
import java.net.Socket

val props = loadProps()

class AsterixDBTSM private constructor(
    override val g: Graph,
    host: String,
    port: String,
    val dataverse: String,
    nodeControllersIPs: List<String>,
    datatype: String,
) : TSManager {

    val clusterControllerHost: String = "http://$host:$port/query/service"
    val asterixHTTPClient: AsterixDBHTTPClient
    val dataFeedIp : String = getDataFeedIP(0, nodeControllersIPs)
    private lateinit var socket: Socket
    private lateinit var outputStream: OutputStream
    private lateinit var writer: PrintWriter
    private var isFeedConnectionOpen: Boolean = false

    private fun getDataFeedIP(id: Long, ipList: List<String>): String {
        val hash = id.hashCode()
        val index = (hash and 0x7FFFFFFF) % ipList.size
        return ipList[index]
    }

    fun openDataFeedConnection(dataFeedPort: Int) {
        try {
            asterixHTTPClient.setupDataFeed()
        } catch (_: Exception) {
            // the datafeed could be already opened and started, if so do nothing
        }
        socket = Socket(dataFeedIp, dataFeedPort)
        outputStream = socket.getOutputStream()
        writer = PrintWriter(outputStream, true)
        isFeedConnectionOpen = true
    }

    fun closeDataFeedConnection(closeRemote: Boolean = false) {
        if (this::writer.isInitialized) {
            writer.close()
            outputStream.close()
            socket.close()
            isFeedConnectionOpen = false
            if (closeRemote) {
                asterixHTTPClient.stopFeed()
                asterixHTTPClient.dropFeed()
            }
        }
    }

    init {
        asterixHTTPClient = AsterixDBHTTPClient(clusterControllerHost, dataFeedIp, incPort(), dataverse, datatype)
    }

    override fun addTS(id: Long): TS {
        if (!isFeedConnectionOpen)
            openDataFeedConnection(asterixHTTPClient.dataFeedPort)
        return AsterixDBTS(g, id + 1,  dataverse, asterixHTTPClient, writer)
    }

    override fun getTS(id: Long): TS {
        if (!isFeedConnectionOpen)
            openDataFeedConnection(asterixHTTPClient.dataFeedPort)
        return AsterixDBTS(g, id, dataverse, asterixHTTPClient, writer)
    }

    override fun clear() {
        closeDataFeedConnection()
        asterixHTTPClient.createTSMEnvironment()
        asterixHTTPClient.initializeTS()
    }

    companion object {
        fun createDefault(g: Graph, dataverse: String = props["default_dataverse"].toString()): AsterixDBTSM {
            return AsterixDBTSM(
                g,
                System.getenv("ASTERIXDB_CC_HOST") ?: "localhost",
                props["default_cc_port"].toString(),
                dataverse,
                System.getenv("DEFAULT_NC_POOL")?.split(',') ?: listOf("localhost"),
                props["default_datatype"].toString(),
            )
        }
    }

    override fun close() {
        closeDataFeedConnection(true)
    }
}
