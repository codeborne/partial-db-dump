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
        tables[it.pkTable]!!.dependants += tables[it.fkTable]!!
        tables[it.fkTable]!!.dependsOn += tables[it.pkTable]!!
      }

      val tableList = topologicalSort(tables.values)
      println("${tables.size} ${tableList.size}")
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
    val result = mutableListOf<Table>()
    val noDepsTables = LinkedList(tables.filter { it.hasNoDependants() })
    while (noDepsTables.isNotEmpty()) {
      val n = noDepsTables.removeFirst()
      result += n
      while (n.dependsOn.isNotEmpty()) {
        val m = n.takeDependency()
        if (m.hasNoDependants())
          noDepsTables += m
      }
    }
    println("${tables.size} ${result.size}")
    println(result)
    val leftDeps = tables.filter { !it.hasNoDependants() }
    if (leftDeps.isNotEmpty())
      throw IllegalStateException("Possible graph cycles, missing tables: $leftDeps")
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
  val dependants = mutableSetOf<Table>()
  val dependsOn =  mutableSetOf<Table>()

  fun hasNoDependants() = dependants.isEmpty()

  fun takeDependency() = dependsOn.first().also {
    dependsOn.remove(it)
    it.dependants.remove(this)
  }

  override fun toString() = "$name -> ${dependsOn.joinToString { it.name }}"
}
