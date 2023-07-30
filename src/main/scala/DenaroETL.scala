import java.io.File
import java.text.SimpleDateFormat
import java.util.Date
import java.sql.Types

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{column, sum, typedLit}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._

object DenaroETL {
  val homePath = System.getenv("HOME")
  val denaroHomePath = s"$homePath/Denaro"

  def main(args: Array[String]): Unit = {
    JdbcDialects.registerDialect(SqliteDialect)

    val session = SparkSession.builder
      .appName("DenaroETL")
      .config("spark.master", "local")
      .getOrCreate()

    import session.implicits._

    val exchangeRates = session
      .read
      .option("multiline", true)
      .option("headers", true)
      .json(s"$homePath/exchange_rates.json")
      .as[ExchangeRate]

    var transactions = getDatabaseFiles()
      .map(db => loadTransactions(session, db))
      .reduce((txDatasetA, txDatasetB) => txDatasetA.union(txDatasetB))

    transactions = transactions
      .join(exchangeRates, exchangeRates("currency") === transactions("currency_code"))
      .withColumn("amount", column("amount") * column("value"))
      .as[Transaction]

    val currentDate = new SimpleDateFormat("yyyy-MM-dd_hh-mm-ss").format(new Date)
    val exportsPath = s"$denaroHomePath/exports"

    transactions
      .repartition(1)
      .write
      .option("headers", true)
      .csv(s"$exportsPath/$currentDate-tx-raw")

    transactions
      .groupBy("group")
      .sum("amount")
      .repartition(1)
      .write
      .option("headers", true)
      .csv(s"$exportsPath/$currentDate-tx-types")

    transactions
      .groupBy("account_name")
      .sum("amount")
      .repartition(1)
      .write
      .option("headers", true)
      .csv(s"$exportsPath/$currentDate-tx-accounts")

    transactions.agg(sum("amount")).show()

    System.exit(0)
  }

  def getDatabaseFiles(): Iterable[String] =
    new File(s"""${System.getenv("HOME")}/Denaro""")
      .listFiles()
      .filter(f => f.getName().endsWith(".nmoney"))
      .map(f => f.getPath())

  def loadTransactions(
      session: SparkSession,
      path: String
  ): Dataset[Transaction] = {
    val query = s"""
      SELECT
        transactions.id,
        transactions.date,
        CASE transactions.type
          WHEN 0 THEN 'Credit'
          WHEN 1 THEN 'Debit'
        END AS type,
        CASE transactions.type
          WHEN 0 THEN amount
          WHEN 1 THEN amount * -1
        END AS amount,
        COALESCE(groups.name, 'Unknown') AS "group",
        metadata.name AS account_name,
        CASE
          WHEN metadata.customCode IS NULL OR LENGTH(TRIM(metadata.customCode)) = 0 THEN 'USD'
          ELSE TRIM(metadata.customCode)
        END AS currency_code
      FROM transactions
      LEFT JOIN groups
        ON groups.id = transactions.gid
      LEFT JOIN metadata
        ON 1 = 1
    """
    import session.implicits._

    println(s"Loading database from ${path}")
    def formatDate(date: String): String = {
      val parsedDate = new SimpleDateFormat("MM/dd/yyyy").parse(date)
      return new SimpleDateFormat("yyyy/MM/dd").format(parsedDate)
    }

    return session.read
      .format("jdbc")
      .option("driver", "org.sqlite.JDBC")
      .option("url", s"jdbc:sqlite:/${path}")
      .option("query", query)
      .load()
      .map(row =>
        Transaction(
          id = row.getAs[String]("id"),
          date = formatDate(row.getAs[String]("date")),
          amount = row.getAs[String]("amount").toDouble,
          transactionType = row.getAs[String]("type"),
          group = row.getAs[String]("group"),
          account_name = row.getAs[String]("account_name"),
          currency_code = row.getAs[String]("currency_code"),
          source_file = path
        )
      )
  }

  case class Transaction(
      id: String,
      date: String,
      transactionType: String,
      amount: Double,
      group: String,
      account_name: String,
      currency_code: String,
      source_file: String
  )

  case class ExchangeRate(
      currency: String,
      value: Double
  )

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
  }
}
