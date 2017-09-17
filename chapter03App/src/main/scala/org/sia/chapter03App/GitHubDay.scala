package org.sia.chapter03App

import org.apache.spark.sql.SparkSession
import scala.collection.JavaConversions._
import java.util.Properties
import scala.io.Source.fromFile


/**
 *
 * # To run
 * $ cd /vagrant/eclipse-workspace/chapter03App
 * $ mvn scala:run -DmainClass=org.sia.chapter03App.GitHubDay -DaddArgs='appHome=/vagrant'
 *
 * https://github.com/spark-in-action/first-edition/blob/master/ch03/scala/org/sia/chapter03App/App.scala
 * https://manning-content.s3.amazonaws.com/download/9/db32c0e-1573-44f5-8468-b09c53fd3229/Zecevic_SparkinAction_err3.html
 *
 * @author ${user.name}
 */
object GitHubDay {

  def main(args: Array[String]) {

    val spark = SparkSession.builder()
      .appName("GitHub push counter")
      .master("local[*]")
      //      .master("spark://192.168.10.2:[*]")
      .getOrCreate();

    val sc = spark.sparkContext;

    val environmentVars = System.getenv()
    //    for ((k, v) <- environmentVars) println(s"key: $k, value: $v")

    val properties = System.getProperties()
    //    for ((k, v) <- properties) println(s"key: $k, value: $v")

    val argMutableMap: collection.mutable.Map[String, String] = collection.mutable.Map()
    args.map(arg => arg.split("=")).foreach(argArr => argMutableMap += argArr(0) -> argArr(1));
    val argMap = argMutableMap.toMap
    println(argMap);

    //    val homeDir = System.getenv("HOME")
    val homeDir = argMap("appHome");
    println("homedir: " + homeDir);
    val inputPath = homeDir + "/sia/github-archive/*.json"

    val empPath = homeDir + "/first-edition/ch03/ghEmployees.txt"
    val employees = Set() ++ (
      for {
        line <- fromFile(empPath).getLines()
      } yield line.trim);
    val broadcastedEmployees = sc.broadcast(employees);

    val isEmp = user => broadcastedEmployees.value.contains(user);
    val isEmployee = spark.udf.register("isEmpUdf", isEmp);

    val ghLog = spark.read.json(inputPath);
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
      filteredEmployees.show(20);
    }
  }
}
