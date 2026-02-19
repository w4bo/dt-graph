package it.unibo.tests.socialnetwork

import org.testcontainers.containers.PostgreSQLContainer
import org.testcontainers.utility.MountableFile
import java.sql.DriverManager
import kotlin.use

class SnbCsvImportTest {
    val postgres = PostgreSQLContainer<Nothing>("postgres:18")
    fun load() {
        postgres.apply {
            withDatabaseName("snb")
            withUsername("test")
            withPassword("test")

            // Copy CSV files into container
            withCopyFileToContainer(
                MountableFile.forHostPath("datasets/dataset/snb/0.3/dynamic/person_0_0.csv"),
                "/tmp/person.csv"
            )
            withCopyFileToContainer(
                MountableFile.forHostPath("datasets/dataset/snb/0.3/dynamic/comment_0_0.csv"),
                "/tmp/comment.csv"
            )
            withCopyFileToContainer(
                MountableFile.forHostPath("datasets/dataset/snb/0.3/dynamic/post_0_0.csv"),
                "/tmp/post.csv"
            )
            withCopyFileToContainer(
                MountableFile.forHostPath("datasets/dataset/snb/0.3/dynamic/post_hasCreator_person_0_0.csv"),
                "/tmp/post_to_person.csv"
            )
            withCopyFileToContainer(
                MountableFile.forHostPath("datasets/dataset/snb/0.3/dynamic/comment_hasCreator_person_0_0.csv"),
                "/tmp/comment_to_person.csv"
            )
        }

        postgres.start()

        DriverManager.getConnection(
            postgres.jdbcUrl,
            postgres.username,
            postgres.password
        ).use { conn ->

            conn.createStatement().use { stmt ->

                // --------------------
                // Create PERSON table
                // --------------------
                stmt.execute("""
                    CREATE TABLE person (
                        id BIGINT PRIMARY KEY,
                        firstName TEXT,
                        lastName TEXT,
                        gender TEXT,
                        birthday BIGINT,
                        creationDate BIGINT,
                        locationIP TEXT,
                        browserUsed TEXT,
                        speaks TEXT,
                        email TEXT
                    );
                """.trimIndent())

                // --------------------
                // Create COMMENT table
                // --------------------
                stmt.execute("""
                    CREATE TABLE comment (
                        id BIGINT PRIMARY KEY,
                        creationDate BIGINT,
                        locationIP TEXT,
                        browserUsed TEXT,
                        content TEXT,
                        length INT
                    );
                """.trimIndent())

                stmt.execute("""
                    CREATE TABLE comment_to_person (
                        start_id BIGINT NOT NULL REFERENCES comment(id),  -- Comment ID
                        end_id BIGINT NOT NULL REFERENCES person(id),    -- Person ID
                        PRIMARY KEY (start_id, end_id)
                    );
                """.trimIndent())

                // --------------------
                // Create POST table
                // --------------------
                stmt.execute("""
                    CREATE TABLE post (
                        id BIGINT PRIMARY KEY,
                        imageFile TEXT,
                        creationDate BIGINT,
                        locationIP TEXT,
                        browserUsed TEXT,
                        language TEXT,
                        content TEXT,
                        length INT
                    );
                """.trimIndent())

                stmt.execute("""
                    CREATE TABLE post_to_person (
                        start_id BIGINT NOT NULL REFERENCES post(id),  -- Comment ID
                        end_id BIGINT NOT NULL REFERENCES person(id),    -- Person ID
                        PRIMARY KEY (start_id, end_id)
                    );
                """.trimIndent())

                // --------------------
                // Load PERSON CSV
                // --------------------
                stmt.execute("""
                    COPY person
                    FROM '/tmp/person.csv'
                    DELIMITER '|'
                    CSV HEADER;
                """.trimIndent())

                // --------------------
                // Load COMMENT CSV
                // --------------------
                stmt.execute("""
                    COPY comment
                    FROM '/tmp/comment.csv'
                    DELIMITER '|'
                    CSV HEADER;
                """.trimIndent())

                stmt.execute("""
                    COPY comment_to_person
                    FROM '/tmp/comment_to_person.csv'
                    DELIMITER '|'
                    CSV HEADER;
                """.trimIndent())

                // --------------------
                // Load POST CSV
                // --------------------
                stmt.execute("""
                    COPY post
                    FROM '/tmp/post.csv'
                    DELIMITER '|'
                    CSV HEADER;
                """.trimIndent())

                stmt.execute("""
                    COPY post_to_person
                    FROM '/tmp/post_to_person.csv'
                    DELIMITER '|'
                    CSV HEADER;
                """.trimIndent())
            }

            // Quick verification
            conn.createStatement().use { stmt ->
                listOf("person", "comment", "post", "comment_to_person", "post_to_person").forEach { table ->
                    val rs = stmt.executeQuery("SELECT COUNT(*) FROM $table")
                    rs.next()
                    println("$table rows: ${rs.getInt(1)}")
                }
            }
        }
    }

    fun stop() {
        postgres.stop()
    }
}