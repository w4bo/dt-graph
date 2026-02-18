package socialnetwork

data class Post(
    val id: Long,
    val imageFile: String?,      // nullable because it can be empty
    val creationDate: Long,
    val locationIP: String,
    val browserUsed: String,
    val language: String,
    val content: String,
    val length: Int
)

fun parsePostLine(line: String): Post {
    val parts = line.split('|')

    return Post(
        id = parts[0].toLong(),
        imageFile = parts[1].ifBlank { null },
        creationDate = parts[2].toLong(),
        locationIP = parts[3],
        browserUsed = parts[4],
        language = parts[5],
        content = parts[6],
        length = parts[7].toInt()
    )
}