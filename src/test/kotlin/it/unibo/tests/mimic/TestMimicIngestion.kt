package it.unibo.tests.mimic

import it.unibo.graph.utils.resetPort
import it.unibo.stats.Loader
import it.unibo.stats.loadDataset
import it.unibo.tests.mimic.loaders.MimicIVNeo4J
import it.unibo.tests.mimic.loaders.MimicIVPGAge
import it.unibo.tests.mimic.loaders.MimicIVSTGraph
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.slf4j.LoggerFactory

private val sizes = listOf(Long.MAX_VALUE, 1_692_200L, 16_922_000L) // listOf()
private val setup: Map<String, List<String>> = mapOf(
    "192.168.30.110" to listOf("192.168.30.110", "192.168.30.110"),
    "192.168.30.101" to listOf("192.168.30.102", "192.168.30.103"),
    "192.168.30.104" to listOf("192.168.30.105", "192.168.30.106", "192.168.30.107", "192.168.30.109")
)

fun load(loader: Loader, model: String, dataset: String, limit: Long, threads: Int = 1, numMachines: Int = -1) {
    loadDataset(loader, model, threads = threads, numMachines = numMachines, dataset, limit.toString())
}

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestMimicIngestion {
    private val logger = LoggerFactory.getLogger(this::class.java)

    @Test
    fun `ingest STGraph`() {
        sizes.forEach { size ->
            load(MimicIVSTGraph(size), "stgraph", "mimic-iv", size, threads = 1)
        }
    }

    @Test
    fun testMimicConcurrent() {
        setup.forEach { (cc, ncs) ->
            sizes.forEach { size ->
                listOf(1, 16).forEach { threads ->
                    logger.info("Size: $size, threads: $threads, nmachines: ${ncs.toSet().size}")
                    resetPort()
                    loadDataset(
                        loader = MimicIVSTGraph(size, threads = threads, host = cc, controllerIPs = ncs),
                        "stgraph",
                        threads = threads,
                        numMachines = ncs.toSet().size,
                        "mimic-iv",
                        size.toString()
                    )
                }
            }
        }
    }
}

fun main() {
    sizes.forEach { limit -> load(MimicIVPGAge(limit), "pgage", "mimic-iv", limit) }
    sizes.forEach { limit -> load(MimicIVNeo4J(limit), "neo4j", "mimic-iv", limit) }
}