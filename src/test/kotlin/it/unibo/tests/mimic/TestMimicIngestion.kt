package it.unibo.tests.mimic

import it.unibo.stats.Loader
import it.unibo.stats.loadDataset
import it.unibo.tests.mimic.loaders.MimicIVNeo4J
import it.unibo.tests.mimic.loaders.MimicIVPGAge
import it.unibo.tests.mimic.loaders.MimicIVSTGraph
import org.junit.jupiter.api.TestInstance
import kotlin.test.Test

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestMimicIngestion {

    val limits = listOf(1_692_200L, 16_922_000L, Long.MAX_VALUE) //4_000)
    val threads = 1
    val machines = 1

    fun load(loader: Loader, model: String, dataset: String, limit: Long) {
        loadDataset(loader, model, threads, machines, dataset, limit.toString())
    }

    @Test
    fun `ingest STGraph`() {
        limits.forEach { limit -> load(MimicIVSTGraph(limit), "stgraph", "mimic-iv", limit) }
    }

    @Test
    fun `ingest Neo4J`() {
        limits.forEach { limit -> load(MimicIVNeo4J(limit), "neo4j", "mimic-iv", limit) }
    }

    @Test
    fun `ingest PGAge`() {
        limits.forEach { limit -> load(MimicIVPGAge(limit), "pgage", "mimic-iv", limit) }
    }
}