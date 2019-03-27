import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.DriverManager
import java.util.*

class Migrator(private val dbUrl: String) {
  fun migrate() {
    DriverManager.getConnection(dbUrl).use { conn ->
      val metaData = conn.metaData
      val tables = tablesByName(metaData)

      val foreignKeys = listForeignKeys(metaData)
      foreignKeys.forEach {
        if (it.pkTable == it.fkTable) println("Warn: $it")
        else {
          tables[it.fkTable]!!.apply {
            this.dependsOn += tables[it.pkTable]!!
            this.foreignKeys += it
          }
        }
      }

      val orderedTablesByDependency = topologicalSort(tables.values)
      println("${tables.size} ${orderedTablesByDependency.size}")
      println(tables.values.joinToString("\n") { "${it.name} ${it.foreignKeys}" })

      val numLastRows = 10
      findForeignKeys(conn, tables, orderedTablesByDependency, numLastRows)
      println(orderedTablesByDependency.joinToString("\n") { "${it.name}: ${it.additionalKeys}" })

      fetchData(conn, orderedTablesByDependency, numLastRows)
    }
  }

  private fun tablesByName(metaData: DatabaseMetaData) =
    metaData.getTables(null, metaData.userName, null, arrayOf("TABLE")).readAll {
      it["TABLE_NAME"]!!.let { name -> Table(name, getPrimaryKeyColumns(metaData, name)) }
    }.associateBy { it.name }

  private fun getPrimaryKeyColumns(metaData: DatabaseMetaData, tableName: String) =
    metaData.getPrimaryKeys(null, metaData.userName, tableName).readAll { it["COLUMN_NAME"] }.toSet()

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

  private fun findForeignKeys(conn: Connection, tables: Map<String, Table>, tableOrder: List<Table>, num: Int) {
    tableOrder.asReversed().forEach { table ->
      if (table.foreignKeys.isNotEmpty()) {
        val columns = table.foreignKeys.map { it.fkColumn }.toSet()
        val sql = """select * from (select ${columns.toQuoted()} from "${table.name}" order by ${table.orderBy} desc) where rownum <= $num"""
        conn.readAll(sql) { rs ->
          table.foreignKeys.forEach {
            rs[it.fkColumn]?.let { value -> tables[it.pkTable]!!.additionalKeys += value }
          }
        }
      }
    }
  }

  private fun fetchData(conn: Connection, tableOrder: List<Table>, num: Int) {
    tableOrder.asReversed().forEach { table ->
      if (table.additionalKeys.isNotEmpty())
        fetchData(conn, """select * from "${table.name}" where ${table.primaryKey.toQuoted()} in (${table.additionalKeys.joinToString()})""", table)
      fetchData(conn, """select * from (select * from "${table.name}" order by ${table.orderBy} desc) where rownum <= $num""", table)
    }
  }

  private fun fetchData(conn: Connection, sql: String, table: Table) {
    conn.readAll(sql) { rs ->
      val metaData = rs.metaData
      val columnNames = (1..metaData.columnCount).map { metaData.getColumnName(it) }.toQuoted()
      val values = (1..metaData.columnCount).map { rs.getObject(it) }.joinToString()
      println("""insert into "${table.name}" (${columnNames}) values (${values})""")
    }
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

data class Table(val name: String, val primaryKey: Set<String>) {
  val dependsOn = mutableSetOf<Table>()
  val foreignKeys = mutableSetOf<ForeignKey>()
  val additionalKeys = mutableSetOf<Any>()
  val orderBy get() = primaryKey.takeIf { it.isNotEmpty() }?.toQuoted() ?: "1"

  override fun toString() = "$name -> ${dependsOn.joinToString { it.name }}"
}

private fun Iterable<String>.toQuoted() = joinToString { """"$it"""" }
