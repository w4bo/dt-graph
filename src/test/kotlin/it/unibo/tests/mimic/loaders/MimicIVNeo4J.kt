package it.unibo.tests.mimic.loaders

import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.Session
import java.sql.ResultSet
import kotlin.math.roundToLong

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

    override fun addTimeseries(row: ResultSet, person: Long) {
        val result = session.run(
            """
            MATCH (p) WHERE id(p) = $person
            CREATE (ts:TimeSeries {
                abbreviation: '${row.getString("abbreviation")}',
                unitname: '${row.getString("unitname")}',
                category: '${row.getString("category")}',
                label: '${row.getString("label")}',
                itemid: '${row.getString("itemid")}',
                param_type: '${row.getString("param_type")}'
            })
            CREATE (p)-[:HAS_PARAMETERS]->(ts)
            RETURN id(ts) AS id
            """.trimIndent()
        )
        currentTSNode = result.single()["id"].asLong()
    }

    override fun addMeasurement(row: ResultSet) {
        val tsNode = currentTSNode ?: return
        val timestamp = s2ts(row.getString("charttime"))
        val value = row.getString("valuenum").toDouble().roundToLong()

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