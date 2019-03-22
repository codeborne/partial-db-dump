import java.sql.ResultSet

fun <T: Any> ResultSet.readAll(mapper: (ResultSet) -> T) = generateSequence {
    if (next()) mapper(this) else null
}

operator fun ResultSet.get(key: String) = getString(key)
