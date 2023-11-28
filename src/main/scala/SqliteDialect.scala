import java.sql.Types

import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._

object SqliteDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.startsWith("jdbc:sqlite")

  override def getCatalystType(
      sqlType: Int,
      typeName: String,
      size: Int,
      md: MetadataBuilder
  ): Option[DataType] =
    sqlType match {
      case Types.DOUBLE => Some(DoubleType)
      case _            => Some(StringType)
    }

  override def getJDBCType(dt: DataType): Option[JdbcType] =
    dt match {
      case DoubleType => Some(JdbcType("DOUBLE", Types.DOUBLE))
      case StringType => Some(JdbcType("VARCHAR", Types.VARCHAR))
      case _          => None
    }

  def autoRegisterDialect: Unit =
    JdbcDialects.registerDialect(SqliteDialect)
}
