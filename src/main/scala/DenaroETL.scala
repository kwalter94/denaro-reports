import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{column, lit, sum, typedLit, max}

object DenaroETL {
  val homePath = System.getenv("HOME")
  val denaroHomePath = s"$homePath/Denaro"
  val denaroExportsPath = s"$denaroHomePath/exports"

  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder
      .appName("DenaroETL")
      .config("spark.master", "local")
      .getOrCreate()

    import session.implicits._

    val exchangeRates = session.read
      .option("multiline", true)
      .option("headers", true)
      .json(s"$homePath/exchange_rates.json")
      .as[ExchangeRate]

    val defaultRate = exchangeRates.where(column("value") === 1.0).first()

    val transactions = databaseFiles
      .map(db => loadTransactions(session, db))
      .reduce((txDatasetA, txDatasetB) => txDatasetA.union(txDatasetB))

    var localisedTransactions = transactions
      .join(exchangeRates)
      .where(exchangeRates("currency") === transactions("currencyCode"))
      .withColumnRenamed("amount", "originalAmount")
      .withColumnRenamed("currencyCode", "originalCurrency")
      .withColumn("amount", column("originalAmount") * column("value"))
      .withColumn("currency", lit(defaultRate.currency))
      .as[LocalisedTransaction]

    processTransactionTypeAggregates(session, localisedTransactions)
    processAccountAggregates(session, localisedTransactions)
    processRawTransactions(session, localisedTransactions)

    System.exit(0)
  }

  def databaseFiles =
    new File(denaroHomePath)
      .listFiles()
      .filter(f => f.getName().endsWith(".nmoney"))
      .map(f => f.getPath())

  def currentDate =
    new SimpleDateFormat("yyyy-MM-dd_hh-mm-ss").format(new Date)

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
    SqliteDialect.autoRegisterDialect

    println(s"Loading database from ${path}")
    def formatDate(date: String): String = {
      val parsedDate = new SimpleDateFormat("MM/dd/yyyy").parse(date)
      return new SimpleDateFormat("yyyy-MM-dd").format(parsedDate)
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
          accountName = row.getAs[String]("account_name"),
          currencyCode = row.getAs[String]("currency_code"),
          sourceFile = path
        )
      )
  }

  def processRawTransactions(
      _session: SparkSession,
      transactions: Dataset[LocalisedTransaction]
  ): Unit = {
    transactions
      .repartition(1)
      .write
      .option("headers", true)
      .csv(s"$denaroExportsPath/$currentDate-tx-raw")

    transactions.agg(sum("amount")).show()
  }

  def processTransactionTypeAggregates(
      session: SparkSession,
      transactions: Dataset[LocalisedTransaction]
  ): Unit = {
    import session.implicits._

    transactions
      .groupByKey(groupMonthlyTransactionsBy(tx => tx.group))
      .agg(sum("amount").as[Double])
      .map(row => (row._1.month, row._1.name, row._2))
      .repartition(1)
      .write
      .option("headers", true)
      .csv(s"$denaroExportsPath/$currentDate-tx-types")
  }

  def processAccountAggregates(
      session: SparkSession,
      transactions: Dataset[LocalisedTransaction]
  ): Unit = {
    import session.implicits._

    transactions
      .groupByKey(groupMonthlyTransactionsBy(tx => tx.accountName))
      .agg(
        sum("amount").as[Double],
        max("currency").as[String],
        sum("originalAmount").as[Double],
        max("originalCurrency").as[String]
      )
      .map(row => (row._1.month, row._1.name, row._2, row._3, row._4, row._5))
      .repartition(1)
      .write
      .csv(s"$denaroExportsPath/$currentDate-tx-accounts")
  }

  def groupMonthlyTransactionsBy(
      bucketingStrategy: LocalisedTransaction => String
  ): LocalisedTransaction => MonthlyTransactionsGrouping =
    (tx: LocalisedTransaction) => {
      val txDate = new SimpleDateFormat("yyyy-MM-dd").parse(tx.date)
      val month = new SimpleDateFormat("yyyy-MM-01").format(txDate)

      MonthlyTransactionsGrouping(month, bucketingStrategy(tx))
    }

  case class Transaction(
      id: String,
      date: String,
      transactionType: String,
      amount: Double,
      group: String,
      accountName: String,
      currencyCode: String,
      sourceFile: String
  )

  case class LocalisedTransaction(
      id: String,
      date: String,
      transactionType: String,
      originalAmount: Double,
      originalCurrency: String,
      amount: Double,
      currency: String,
      group: String,
      accountName: String,
      sourceFile: String
  )

  case class ExchangeRate(
      currency: String,
      value: Double
  )

  case class MonthlyTransactionsGrouping(
      month: String,
      name: String
  )
}
