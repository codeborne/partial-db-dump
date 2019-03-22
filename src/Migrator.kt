import java.sql.DatabaseMetaData
import java.sql.DriverManager

class Migrator(private val dbUrl: String) {
  fun migrate() {
    DriverManager.getConnection(dbUrl).use { conn ->
      val metaData = conn.metaData
      val tables = listTables(metaData)
      val foreignKeys = listForeignKeys(metaData)

      foreignKeys.forEach {
        tables[it.pkTable]!!.deps += tables[it.fkTable]!!
      }

      println(tables.values.joinToString("\n"))
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
  val deps = mutableListOf<Table>()

  override fun toString() = "$name <- ${deps.joinToString { it.name }}"
}
