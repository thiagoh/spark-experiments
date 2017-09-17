package org.sia.chapter03App

import org.apache.spark.sql.SparkSession
import scala.collection.JavaConversions._
import java.util.Properties
import scala.io.Source.fromFile

/**
 *
 * # To run
 * $ cd /vagrant/eclipse-workspace/chapter03App
 * $ mvn -Dmaven.test.skip=true -DskipTests clean package
 * $ spark-submit --class org.sia.chapter03App.GitHubDaySparkSubmit --master "local[*]" \
 * 	 --name "Daily Github Push Counter" target/chapter03App-0.0.1-SNAPSHOT.jar \
 *   "/vagrant/sia/github-archive/*.json" "/vagrant/first-edition/ch03/ghEmployees.txt" \
 *   "/vagrant/sia/emp-gh-push-output" "json"
 * */
 *
 * https://github.com/spark-in-action/first-edition/blob/master/ch03/scala/org/sia/chapter03App/App.scala
 * https://manning-content.s3.amazonaws.com/download/9/db32c0e-1573-44f5-8468-b09c53fd3229/Zecevic_SparkinAction_err3.html
 *
 * @author ${user.name}
 */
object Chapter04Script {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().getOrCreate();

    val sc = spark.sparkContext;

    val tranFile = sc.textFile("first-edition/ch04/ch04_data_transactions.txt");
    val tranData = tranFile.map(_.split('#'));
    var transByCust = tranData.map(tran => (tran(2).toInt, tran));
    println(transByCust);

    val custIdsAll = transByCust.keys;
    val custIds = custIdsAll.distinct();

    println("##########################################");
    println("customer ids all: " + custIdsAll.count());
    println("customer ids distict: " + custIds.count());

    println("##########################################");
    println("customer ids top(10): " + custIdsAll.top(10));
    println("customer ids top(10): " + custIds.top(10));

    println("get prices per key from 0 to 10): ");
    transByCust.map(pair => (pair._1, pair._2(5))).collect().slice(0, 10).foreach(println)

    println("transByCust.countByKey()" + transByCust.countByKey());
    println("sum by sum: " + transByCust.countByKey().values.sum);
    println("sum by reduce: " + transByCust.countByKey.values.reduce((i, m) => i + m));

  }
}
