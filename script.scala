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
val last = transByCustCountByKey.toSeq.sortBy(_._2).last;
println("customer .last :" + last);
transByCust.lookup(last._1);

var complTrans = Array(Array("2015-03-30", "11:59 PM", "53", "4", "1", "0.00"));