package it.unibo.tests.mimic

import org.neo4j.driver.*
import kotlin.math.roundToLong

class MimicIVNeo4J(
    limit: Long,
    uri: String = "bolt://localhost:7687",
    user: String = "neo4j",
    password: String = "password"
) : AbstractMimicIVLoader(limit) {

    private val driver: Driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password))
    private val session: Session
    private var currentTSNode: Long? = null

    init {
        session = driver.session()
        clearGraph()
    }

    /** Deletes all nodes and relationships in the database */
    private fun clearGraph() {
        session.run("MATCH (n) DETACH DELETE n")
    }

    override fun addPerson(subjectId: Int): Long {
        val result = session.run(
            """
            CREATE (p:Person {subject_id: ${subjectId}})
            RETURN id(p) AS id
            """.trimIndent()
        )

        return result.single()["id"].asLong()
    }

    override fun addTimeseries(row: Map<String, Any?>, person: Long) {
        val result = session.run(
            """
            MATCH (p) WHERE id(p) = $person
            CREATE (ts:Timeseries {
                abbreviation: '${row["abbreviation"]}',
                unitname: '${row["unitname"]}',
                category: '${row["category"]}',
                label: '${row["label"]}',
                itemid: '${row["itemid"]}',
                param_type: '${row["param_type"]}'
            })
            CREATE (p)-[:HAS_PARAMETERS]->(ts)
            RETURN id(ts) AS id
            """.trimIndent()
        )
        currentTSNode = result.single()["id"].asLong()
    }

    override fun addMeasurement(row: Map<String, Any?>) {
        val tsNode = currentTSNode ?: return
        val timestamp = s2ts(row["charttime"].toString())
        val value = row["valuenum"].toString().toDouble().roundToLong()

        session.run(
            """
            MATCH (ts) WHERE id(ts) = $tsNode
            CREATE (m:Measurement {
                timestamp: $timestamp,
                value: $value
            })
            CREATE (ts)-[:HAS_MEASUREMENT]->(m)
            """.trimIndent()
        )
    }

    override fun close() {
        session.close()
        driver.close()
    }
}