import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLException

fun <T : Any> ResultSet.readAll(mapper: (ResultSet) -> T) = use {
  generateSequence {
    if (next()) mapper(this) else null
  }.toList()
}

fun <T: Any> PreparedStatement.readAll(mapper: (ResultSet) -> T) = use {
  executeQuery().readAll(mapper)
}

fun <T: Any> Connection.readAll(sql: String, mapper: (ResultSet) -> T) = try {
  prepareStatement(sql).readAll(mapper)
}
catch (e: SQLException) {
  throw SQLException("Failed to execute:\n$sql", e)
}

operator fun ResultSet.get(key: String) = getString(key)
operator fun ResultSet.get(n: Int) = getObject(n)
