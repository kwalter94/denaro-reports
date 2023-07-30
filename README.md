# Denaro Reports

Prepares a bunch of aggregate reports from all my financial transactions
tracked by [Denaro](https://github.com/NickvisionApps/Denaro) and helps
me get comfortable working with Apache Spark. Two stones, one bird really
(I am the bird getting stoned here).

## Building and Running

- Bulding:
    ```sh
    $ sbt assembly
    ```
- Running:
    ```sh
    $ spark-submit --class DenaroETL --master local target/scala-2.12/DenaroETL-assembly-1.0.jar
    ```