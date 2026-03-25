package it.unibo.tests.ci.smartbench

import it.unibo.graph.utils.resetPort
import it.unibo.stats.loadDataset
import it.unibo.tests.ci.smartbench.loaders.SmartBenchDataLoader
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory

class TestSmartBenchIngestion {
    private val logger = LoggerFactory.getLogger(this::class.java)
    val sizes = listOf("small") // , "medium", "large"

    @Test
    fun testSmartBenchIngestion() {
        sizes.forEach { size ->
            val threads = 1
            logger.info("\n--- $size")
            resetPort()
            loadDataset(
                loader = SmartBenchDataLoader(size, threads = threads),
                "stgraph",
                threads = threads,
                numMachines = 1,
                "smartbench",
                size
            )
        }
    }

    @Test
    fun testSmartBenchConcurrent() {
        val setup: Map<String, List<String>> = mapOf(
            "192.168.30.110" to listOf("192.168.30.110", "192.168.30.110"),
            "192.168.30.101" to listOf("192.168.30.102", "192.168.30.103"),
            "192.168.30.104" to listOf("192.168.30.105", "192.168.30.106", "192.168.30.107", "192.168.30.109")
        )
        setup.forEach { (cc, ncs) ->
            sizes.forEach { size ->
                val threads = 1
                logger.info("\n--- $size")
                resetPort()
                loadDataset(
                    loader = SmartBenchDataLoader(size, threads = threads, host = cc, controllerIPs = ncs),
                    "stgraph",
                    threads = threads,
                    numMachines = ncs.size,
                    "smartbench",
                    size
                )
            }
        }
    }
}