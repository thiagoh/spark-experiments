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
println("customer ids top(20): ");
custIdsAll.top(20).foreach(s => print(s + ", "));
println();
println("customer ids top(20): ");
custIds.top(20).foreach(s => print(s + ", "));
println();

println("##########################################");
println("get prices per key from 0 to 10): ");
transByCust.map(pair => (pair._1, pair._2(5))).collect().slice(0, 10).foreach(println);

println("##########################################");
val transByCustCountByKey = transByCust.countByKey();
println("transByCust.countByKey()" + transByCustCountByKey);
println("sum by sum: " + transByCustCountByKey.values.sum);
println("sum by reduce: " + transByCust.countByKey.values.reduce((i, m) => i + m));

println("##########################################");
// transByCustCountByKey.toSeq;
val head = transByCustCountByKey.toSeq.sortBy(_._2).head
println("customer .head :" + head);
transByCust.lookup(head._1);
transByCust.lookup(head._1).foreach(trans => println(trans.mkString(", ")));
val last = transByCustCountByKey.toSeq.sortBy(_._2).last;
println("customer .last :" + last);
transByCust.lookup(last._1);
transByCust.lookup(last._1).foreach(trans => println(trans.mkString(", ")));

var complTrans = Array(Array("2015-03-30", "11:59 PM", "53", "4", "1", "0.00"));

println("##########################################");
println("Before applying 5% of discount on product with ID of 25");
transByCust.filter(pair => pair._2(3).toInt == 25).foreach(pair => println(pair._1 + ": " + pair._2.mkString(", ")));
println("After applying 5% of discount on product with ID of 25");
transByCust = transByCust.mapValues(transaction => {
  
  if (transaction(3).toInt == 25 // prod id
      && transaction(4).toDouble >= 2) { // prod quantity
    transaction(5) = (transaction(5).toDouble * 0.95).toString // apply 5% discount
  }
  transaction // return transaction
});

transByCust.filter(pair => pair._2(3).toInt == 25).foreach(pair => {
  print(pair._1 + ": " + pair._2.mkString(", "))
  if (pair._2(4).toDouble >= 2) print(" // discount applied");
  println();
});

println();
println("##########################################");
println("How many transactions? " + transByCust.count());
println("Apply promotions transformations");
transByCust = transByCust.flatMapValues(transaction => {
  if (transaction(3).toInt == 81 // product 81
      && transaction(4).toDouble >= 5) { // quantity
    val cloned = transaction.clone();
    cloned(5) = "0.00"; // price
    cloned(3) = "70"; // product id (toothbrush)
    cloned(4) = "1"; // quantity
    List(transaction, cloned);
  } else {
    List(transaction);
  }
});
println("How many transactions? " + transByCust.count());

println();
println("##########################################");
println("Sum purchases by customer ID using reduceByKey ");
transByCust.reduceByKey((transaction1, transaction2) => {
  transaction1(0) = "2015-03-30" // setting date
  transaction1(1) = "11:59 PM" // setting time
  transaction1(3) = transaction1(3) + ";" + transaction2(3); // setting ids
  transaction1(4) = (transaction1(4).toInt + transaction2(4).toInt).toString; // setting quantity (summed up)
  transaction1(5) = ( // setting final cost
      transaction1(5).toDouble
      + transaction2(5).toDouble
    ).toString;
  transaction1
}).foreach(pair => println(pair._1 + ": " + pair._2.mkString(", ")));
