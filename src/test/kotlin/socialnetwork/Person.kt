package socialnetwork

data class Person(
    val id: Long,
    val firstName: String,
    val lastName: String,
    val gender: String,
    val birthday: Long,
    val creationDate: Long,
    val locationIP: String,
    val browserUsed: String,
    val speaks: List<String>,
    val email: List<String>
)

fun parsePersonLine(line: String): Person {
    val parts = line.split("|")

    return Person(
        id = parts[0].toLong(),
        firstName = parts[1],
        lastName = parts[2],
        gender = parts[3],
        birthday = parts[4].toLong(),
        creationDate = parts[5].toLong(),
        locationIP = parts[6],
        browserUsed = parts[7],
        speaks = if (parts[8].isNotBlank()) parts[8].split(";") else emptyList(),
        email = if (parts[9].isNotBlank()) parts[9].split(";") else emptyList()
    )
}