package it.unibo.tests.socialnetwork

data class Comment(
    val id: Long,
    val creationDate: Long,
    val locationIP: String,
    val browserUsed: String,
    val content: String,
    val length: Int
)

fun parseCommentLine(line: String): Comment {
    val parts = line.split('|')

    return Comment(
        id = parts[0].toLong(),
        creationDate = parts[1].toLong(),
        locationIP = parts[2],
        browserUsed = parts[3],
        content = parts[4],
        length = parts[5].toInt()
    )
}