package it.unibo.tests.mimic

import java.sql.Connection
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.SQLException
import kotlin.math.roundToLong

class MimicIVPGAge(
    limit: Long,
    url: String = "jdbc:postgresql://localhost:5435/mimic-iv",
    user: String = "postgres",
    password: String = "password"
) : AbstractMimicIVLoader(limit)  {
    private val graphName = "mimic_${if (limit == Long.MAX_VALUE) "full" else limit}"
    private val conn: Connection = DriverManager.getConnection(url, user, password)
    private var currentTSId: Long? = null
    private lateinit var insertMeasurement: PreparedStatement
    init {
        conn.createStatement().use { st ->
            // Helper function for safe execution
            fun safeExec(sql: String) {
                try {
                    st.execute(sql)
                } catch (e: SQLException) {
                    println("Warning executing SQL: $sql")
                    e.printStackTrace()
                }
            }

            // Load extensions
            safeExec("CREATE EXTENSION IF NOT EXISTS age")
            safeExec("CREATE EXTENSION IF NOT EXISTS timescaledb")
            safeExec("LOAD 'age';")

            // Set search path to allow AGE operations
            safeExec("SET search_path = ag_catalog, \"$user\", public;")

            // Drop the graph if it exists
            try {
                st.execute("SELECT drop_graph('$graphName', true)")
            } catch (e: SQLException) {
                println("Graph $graphName did not exist, skipping drop.")
            }

            // Create graph safely
            safeExec("SELECT create_graph('$graphName')")

            // Switch to the graph schema for subsequent operations
            safeExec("SET search_path = $graphName, \"$user\", public;")

            safeExec("""
                CREATE TABLE IF NOT EXISTS measurements (
                    ts_id BIGINT,
                    timestamp BIGINT,
                    value BIGINT
                )""".trimIndent())

            safeExec("TRUNCATE TABLE Measurements")

            // Convert table to hypertable (Timescale)
            safeExec("""
                SELECT create_hypertable(
                    'measurements',
                    'timestamp',
                    if_not_exists => TRUE
                )
                """.trimIndent())
        }

        // Prepare statement for inserting measurements
        insertMeasurement = conn.prepareStatement("""INSERT INTO ${graphName}.measurements(ts_id, timestamp, value) VALUES (?, ?, ?)""".trimIndent())
    }

    override fun addPerson(subjectId: Int): Long {
        conn.createStatement().use { st ->
            st.execute("LOAD 'age';")
            st.execute("SET search_path = ag_catalog, public;")
            val rs = st.executeQuery(
                """
                SELECT *
                FROM cypher('$graphName', $$
                    CREATE (p:Person {subject_id:$subjectId})
                    RETURN id(p)
                $$) as (id agtype)
                """.trimIndent())
            rs.next()
            return rs.getString(1).replace("\"", "").toLong()
        }
    }

    override fun addTimeseries(row: Map<String, Any?>, person: Long) {
        val abbreviation = row["abbreviation"]
        val unitname = row["unitname"]
        val category = row["category"]
        val label = row["label"]
        val itemid = row["itemid"]
        val paramType = row["param_type"]

        conn.createStatement().use { st ->
            st.execute("LOAD 'age';")
            st.execute("SET search_path = ag_catalog, public;")
            val rs = st.executeQuery(
                """
            SELECT *
            FROM cypher('$graphName', $$
                MATCH (p) WHERE id(p) = $person
                CREATE (ts:Timeseries {
                    abbreviation:"$abbreviation",
                    unitname:"$unitname",
                    category:"$category",
                    label:"$label",
                    itemid:$itemid,
                    param_type:"$paramType"
                })
                CREATE (p)-[:HAS_PARAMETERS]->(ts)
                RETURN id(ts)
            $$) as (id agtype)
            """.trimIndent()
            )
            rs.next()
            currentTSId = rs.getString(1).replace("\"","").toLong()
        }
    }

    override fun addMeasurement(row: Map<String, Any?>) {
        val tsId = currentTSId ?: return
        val timestamp = s2ts(row["charttime"].toString())
        val value = row["valuenum"].toString().toDouble().roundToLong()
        insertMeasurement.setLong(1, tsId)
        insertMeasurement.setLong(2, timestamp)
        insertMeasurement.setLong(3, value)
        insertMeasurement.executeUpdate()
    }

    override fun close() {
        insertMeasurement.close()
        conn.close()
    }
}