package it.unibo.tests.mimic.loaders

import it.unibo.tests.smartbench.loaders.TSRecord
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.Session

fun limitToPort(limit: Long): Int {
    return if (limit < 5_000_000) {
        8687
    } else if (limit < 50_000_000) {
        8688
    } else {
        8689
    }
}

class MimicIVNeo4J(
    limit: Long,
    uri: String = "bolt://localhost:${limitToPort(limit)}",
    user: String = "neo4j",
    password: String = "password"
) : AbstractMimicIVLoader(limit, threads = 1) {

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

    override fun addTimeseries(abbreviation: String, unitname: String?, category: String, label: String, itemid: Int, person: Long): Long {
        val result = session.run(
            """
            MATCH (p) WHERE id(p) = $person
            CREATE (ts:TimeSeries {
                abbreviation: '${abbreviation}',
                unitname: '${unitname}',
                category: '${category}',
                label: '${label}',
                itemid: '${itemid}'
            })
            CREATE (p)-[:HAS_PARAMETERS]->(ts)
            RETURN id(ts) AS id
            """.trimIndent()
        )
        return result.single()["id"].asLong()
    }

    override fun addMeasurement(tsId: Long, row: TSRecord, isLast: Boolean) {
        session.run(
            """
            MATCH (ts) WHERE id(ts) = $tsId
            CREATE (m:${row.type} {
                timestamp: ${row.timestamp},
                value: ${row.value}
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