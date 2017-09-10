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
 */
 *
 * https://github.com/spark-in-action/first-edition/blob/master/ch03/scala/org/sia/chapter03App/App.scala
 * https://manning-content.s3.amazonaws.com/download/9/db32c0e-1573-44f5-8468-b09c53fd3229/Zecevic_SparkinAction_err3.html
 *
 * @author ${user.name}
 */
object GitHubDaySparkSubmit {

  def main(args: Array[String]) {

    val spark = SparkSession.builder().getOrCreate();

    val sc = spark.sparkContext;

    val employees = Set() ++ (
      for {
        line <- fromFile(args(1)).getLines()
      } yield line.trim);
    val broadcastedEmployees = sc.broadcast(employees);

    val isEmp = user => broadcastedEmployees.value.contains(user);
    val isEmployee = spark.udf.register("isEmpUdf", isEmp);

    val ghLog = spark.read.json(args(0));
    val pushes = ghLog.filter("type = 'PushEvent'")
    pushes.printSchema();

    println("all events: " + ghLog.count());
    println("pushes events: " + pushes.count());

    pushes.show(5);
    pushes.drop("public", "org").show(5);

    val USERNAME_COLUMN_NAME = "username";

    val grouped = pushes.groupBy(pushes("actor.login")
      .as(USERNAME_COLUMN_NAME)).count();

    println("grouped by actor: ");
    grouped.show(5);

    val ordered = grouped.orderBy(grouped("count").desc);
    println("ordered by: ");
    ordered.drop("public", "org").show(5);

    val ordered2 = grouped.orderBy(grouped("count").desc, grouped(USERNAME_COLUMN_NAME).asc);
    println("ordered by 2 columns: ");
    ordered2.drop("public", "org").show(20);

    {
      val filteredEmployees = pushes.filter("isEmpUdf(actor.login)");
      val pushedFilteredByEmp = filteredEmployees.groupBy(filteredEmployees("actor.login").as("username")).count();
      println("filtered pushes events: " + pushedFilteredByEmp.count());
      val orderedPushedFilteredByEmp = pushedFilteredByEmp.orderBy(pushedFilteredByEmp("username").asc)
      orderedPushedFilteredByEmp.drop("public", "org").show(20);
    }

    println();
    println();
    println();
    println();

    {
      import spark.implicits._;
      val filteredEmployees = ordered.filter(isEmployee($"username"));
      filteredEmployees.write.format(args(3)).save(args(2))
    }
  }
}
