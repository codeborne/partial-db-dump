import java.sql.DatabaseMetaData
import java.sql.DriverManager

class Migrator(private val dbUrl: String) {
  fun migrate() {
    DriverManager.getConnection(dbUrl).use { conn ->
      val metaData = conn.metaData
      val tables = listTables(metaData)
      val foreignKeys = listForeignKeys(metaData)
      val tableDependencies = foreignKeys.groupBy { it.pkTable }

      println(tableDependencies.entries.joinToString("\n"))

      val tablesNoDeps = tables.filter { !tableDependencies.containsKey(it) }.toList()
      println(tablesNoDeps.size)
    }
  }

  private fun listTables(metaData: DatabaseMetaData) =
    metaData.getTables(null, metaData.userName, null, arrayOf("TABLE")).readAll {
      it["TABLE_NAME"]
    }

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