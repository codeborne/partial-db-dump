import java.sql.ResultSet

fun <T : Any> ResultSet.readAll(mapper: (ResultSet) -> T) = use {
  generateSequence {
    if (next()) mapper(this) else null
  }.toList()
}

operator fun ResultSet.get(key: String) = getString(key)
operator fun ResultSet.get(n: Int) = getObject(n)
