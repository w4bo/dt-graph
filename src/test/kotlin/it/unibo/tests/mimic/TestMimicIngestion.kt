package it.unibo.tests.mimic

import it.unibo.stats.TestConfig
import it.unibo.tests.mimic.loaders.MimicIVSTGraph
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestMimicIngestion {
    @Test
    fun `ingest MIMIC`() {
        TestConfig.runIngestion("mimic") { size, threads, host, controllerIPs ->
            MimicIVSTGraph(size, threads = threads, host = host, controllerIPs = controllerIPs)
        }
    }
}

//fun main() {
//    mimic_sizes.forEach { limit -> load(MimicIVPGAge(limit), "pgage", "mimic", limit) }
//    mimic_sizes.forEach { limit -> load(MimicIVNeo4J(limit), "neo4j", "mimic", limit) }
//}