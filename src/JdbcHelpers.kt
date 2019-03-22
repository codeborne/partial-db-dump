import java.sql.PreparedStatement
import java.sql.ResultSet

fun <T : Any> ResultSet.readAll(mapper: (ResultSet) -> T) = use {
  generateSequence {
    if (next()) mapper(this) else null
  }.toList()
}

fun <T: Any> PreparedStatement.readAll(mapper: (ResultSet) -> T) = use {
  executeQuery().readAll(mapper)
}

operator fun ResultSet.get(key: String) = getString(key)
operator fun ResultSet.get(n: Int) = getObject(n)
