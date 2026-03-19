package it.unibo.tests.mimic

import it.unibo.stats.Loader
import it.unibo.stats.loadDataset
import org.junit.jupiter.api.TestInstance
import kotlin.test.Test

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestMimicIngestion {

    val limit = 100L
    val threads = 1
    val machines = 1

    fun load(loader: Loader, model: String, dataset: String) {
        loadDataset(loader, model, threads, machines, dataset, limit.toString())
    }

    @Test
    fun `ingest STGraph`() {
        load(MimicIVSTGraph(limit), "stgraph", "mimic-iv")
    }

    @Test
    fun `ingest Neo4J`() {
        load(MimicIVNeo4J(limit), "neo4j", "mimic-iv")
    }

    @Test
    fun `ingest PGAge`() {
        load(MimicIVPGAge(limit), "pgage", "mimic-iv")
    }
}