import java.lang.IllegalStateException
import java.sql.DatabaseMetaData
import java.sql.DriverManager
import java.util.*

class Migrator(private val dbUrl: String) {
  fun migrate() {
    DriverManager.getConnection(dbUrl).use { conn ->
      val metaData = conn.metaData
      val tables = listTables(metaData)

      val foreignKeys = listForeignKeys(metaData)
      foreignKeys.forEach {
        if (it.pkTable == it.fkTable) println("Warn: $it")
        else tables[it.fkTable]!!.dependsOn += tables[it.pkTable]!!
      }

      val orderedTablesByDependency = topologicalSort(tables.values)
      println("${tables.size} ${orderedTablesByDependency.size}")
      println(orderedTablesByDependency.joinToString("\n"))
    }
  }

  private fun listTables(metaData: DatabaseMetaData) =
    metaData.getTables(null, metaData.userName, null, arrayOf("TABLE")).readAll {
      Table(it["TABLE_NAME"])
    }.associateBy { it.name }

  private fun listForeignKeys(metaData: DatabaseMetaData) =
    metaData.getImportedKeys(null, metaData.userName, null).readAll {
      ForeignKey(it["FKTABLE_NAME"], it["FKCOLUMN_NAME"], it["PKTABLE_NAME"], it["PKCOLUMN_NAME"])
    }

  private fun topologicalSort(tables: Collection<Table>): List<Table> {
    val result = LinkedList<Table>()
    val marked = mutableMapOf<Table, Boolean>()

    fun visit(n: Table) {
      if (marked[n] == true) return
      if (marked[n] == false) throw IllegalStateException("Not a DAG: $n\nGot so far: $result")
      marked[n] = false
      n.dependsOn.forEach { visit(it) }
      marked[n] = true
      result += n
    }

    while (marked.size < tables.size) {
      tables.find { !marked.containsKey(it) }?.let { visit(it) }
    }
    return result
  }
}

data class ForeignKey(
  val fkTable: String,
  val fkColumn: String,
  val pkTable: String,
  val pkColumn: String
) {
  override fun toString() = "$fkTable.$fkColumn -> $pkTable.$pkColumn"
}

data class Table(val name: String) {
  val dependsOn = mutableSetOf<Table>()

  override fun toString() = "$name -> ${dependsOn.joinToString { it.name }}"
}
