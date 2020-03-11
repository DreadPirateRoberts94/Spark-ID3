package id3

import org.apache.spark

import scala.collection.mutable
import java.io.{File, PrintWriter}

object ID3 {
  //type alias
  type classCountLevel1 = mutable.HashMap[Int, mutable.HashMap[String, mutable.HashMap[String, Int]]]
  type classCountLevel2 = mutable.HashMap[String, mutable.HashMap[String, Int]]
  type classCountLevel3 = mutable.HashMap[String, Int]

  def main(args: Array[String]): Unit ={

    //spark setup
    val conf = new spark.SparkConf().setAppName("portoID3").setMaster("local[*]")
    val sc = new spark.SparkContext(conf)

    sc.setLogLevel("ERROR")

    //setup data tennis
    val (translateAttr, values, targetAttribute, attributes) = parseHeader(args(0), sc)

    val dataRdd = sc.textFile(args(1)).map(row => row.split(","))

    //algorithm setup
    val tree = mutable.HashMap.empty[Int, Node]
    //create root
    val root = new Node(values.values.map(_.size).max, id=0, -1, attributes)
    //add root
    tree(0) = root
    var nodesToProcess = List[Node](root)

    val elapsedTime = time{

      while(nodesToProcess.nonEmpty){

        val countTables = dataRdd.mapPartitions{
          partition =>
            processPartition(partition, nodesToProcess, tree,
              attributes, values, targetAttribute)

        }.reduceByKey{  //reduce
          (table1, table2) =>
            reduceTable(table1, table2)
        }.toLocalIterator

        //tmp buffer
        var nodesToAdd = List.empty[Node]

        countTables.foreach{
          case (id, countTable) =>
            nodesToAdd = processTable(id, countTable, tree, values, targetAttribute) ::: nodesToAdd
        }
        nodesToProcess = nodesToAdd
      }
    }

    val writer = new PrintWriter(new File(args(2)))

    writer.write("Elapsed time: " + elapsedTime + "\n")
    writer.write("\n---new tree---\n\n")
    writer.write(printTree(tree, translateAttr, values, 0, 0))
    writer.write("\n---end tree---")

    println("Tree available in " + args(2))
    writer.close()
  }

  def parseHeader(path: String, sc: spark.SparkContext): (Map[Int,String], Map[Int, Vector[String]], Int, Set[Int]) = {

    var index = 0
    val translateAttr = mutable.Map[Int,String]()
    val values = mutable.Map[Int, Vector[String]]()
    var targetAttribute = -1
    var attributes = Set[Int]()

    sc.textFile(path, 150).collect().foreach {
      case "" =>
      case classValues if classValues.startsWith("Class values: ") =>
        values.put(index, classValues.replaceAll("Class values: ", "").split(",").map(_.replace(" ", "")).toVector)
        targetAttribute = index
      case attributes if attributes.startsWith("Attributes values: ") =>
      case nameFile if nameFile.startsWith("Name File: ") =>
      case attributeAndValues =>
        translateAttr.put(index, attributeAndValues.split(":")(0))
        values.put(index, attributeAndValues.split(":")(1).split(",").map(_.replace(" ", "")).toVector)
        index += 1
    }

    attributes = values.keys.toSeq.sorted.slice(0, values.keys.size-1).toSet
    (translateAttr.toMap, values.toMap, targetAttribute, attributes)
  }

  def processPartition(
                        partition: Iterator[Array[String]],
                        nodesToProcess: List[Node],
                        tree: mutable.HashMap[Int, Node],
                        attributes: Set[Int],
                        values: Map[Int, Vector[String]],
                        targetAttribute: Int
                      ): Iterator[(Int, classCountLevel1)] =
  {
    val partialClassCounts = new mutable.HashMap[Int, classCountLevel1]()

    //setup
    nodesToProcess.foreach{node => setup(node,partialClassCounts,attributes,values,targetAttribute)}

    //map
    partition.foreach{row => processRow(row, partialClassCounts,tree, values, targetAttribute)}

    partialClassCounts.iterator
  }

  //setup
  def setup(
             node: Node,
             partialClassCounts: mutable.HashMap[Int, classCountLevel1],
             attributes: Set[Int],
             values: Map[Int, Vector[String]],
             targetAttribute: Int
           ): Unit =
  {
    partialClassCounts(node.id) = new classCountLevel1()
    attributes.
      foreach{attr =>
        partialClassCounts(node.id)(attr) = new classCountLevel2()
        values(attr)
          .foreach{va =>
            partialClassCounts(node.id)(attr)(va) = new classCountLevel3()
            values(targetAttribute)
              .foreach(vc => partialClassCounts(node.id)(attr)(va)(vc) = 0)
          }
      }
  }

  //process row
  def processRow(
                  row: Array[String],
                  partialClassCounts: mutable.HashMap[Int, classCountLevel1],
                  tree: mutable.HashMap[Int, Node],
                  values: Map[Int, Vector[String]],
                  targetAttribute: Int
                ): Unit =
  {
    //per ogni riga bisogna visitare l'albero e capire in che nodo si trova e recuperare così l'id
    val nodeId = classifyRow(row, tree, values, 0)
    if(partialClassCounts.contains(nodeId)){
      //se è vuoto, bisogna prendere l'attribute del padre
      if(tree(nodeId).unusedAttributes.nonEmpty){
        tree(nodeId).unusedAttributes.foreach{
          attribute => partialClassCounts(nodeId)(attribute)(row(attribute))(row(targetAttribute)) += 1
        }
      }else{
        //empty, allora devo prendere l'attribute del padre
        val attribute = tree(tree(nodeId).parentId).attribute
        partialClassCounts(nodeId)(attribute)(row(attribute))(row(targetAttribute)) += 1
      }
    }
  }

  //reduce tables
  def reduceTable(table1: classCountLevel1, table2: classCountLevel1): classCountLevel1 ={
    table1.keys.foreach{
      keyL1 =>
        table1(keyL1).keys.foreach{
          keyL2 =>
            table1(keyL1)(keyL2).keys.foreach{
              keyL3 =>
                table1(keyL1)(keyL2)(keyL3) = table1(keyL1)(keyL2)(keyL3) + table2(keyL1)(keyL2)(keyL3)
            }
        }
    }
    table1
  }

  def processTable(
                    id: Int,
                    countTable: classCountLevel1,
                    tree: mutable.HashMap[Int, Node],
                    values: Map[Int, Vector[String]],
                    targetAttribute: Int
                  ): List[Node] =
  {
    var nodesToAdd = List.empty[Node]
    //bisogna fare un controllo sugli unusedAttributes
    if(tree(id).unusedAttributes.isEmpty){
      val (_, mostCommonClassLabel) = checkMostCommonClass(values(targetAttribute),countTable)
      tree(id).setIsleaf(true)
      tree(id).setLeafLabel(mostCommonClassLabel)
    } else {
      val aBest = tree(id).unusedAttributes.map(attr => infoGain(attr, countTable)).min._2
      tree(id).setAttribute(aBest)
      //ora devo controllare se è una foglia e prendere la classe più comune
      val (isLeaf, mostCommonClassLabel) = checkMostCommonClass(values(targetAttribute),countTable)
      //aggiorno albero
      tree(id).setIsleaf(isLeaf)
      tree(id).setLeafLabel(mostCommonClassLabel)

      if(!tree(id).isLeaf) {
        val children = tree(id).genChildren(values(aBest).size)
        //aggiorno lista
        nodesToAdd = children ::: nodesToAdd
        //aggiorno albero
        children.foreach(node => tree(node.id) = node)
      }
    }
    nodesToAdd
  }

  @scala.annotation.tailrec
  def classifyRow(row: Array[String], tree: mutable.HashMap[Int, Node], values: Map[Int, Vector[String]], nodeId: Int): Int ={
    if(tree(nodeId).isLeaf)
      nodeId
    else
      classifyRow(row, tree, values, tree(nodeId).children(values(tree(nodeId).attribute).indexOf(row(tree(nodeId).attribute))))
  }

  def log2(x: Int): Double ={
    val tmpLog = math.log10(x.toDouble)/math.log10(2.0)
    if(tmpLog.isInfinity || tmpLog.isNaN) 0.0 else tmpLog
  }

  //calcolo infogain
  def infoGain(attribute: Int, countTable: classCountLevel1): (Double, Int) ={
    var infoG = 0.0
    for(keyL2 <- countTable(attribute).keys){
      val all = countTable(attribute)(keyL2).values.sum
      infoG += all*log2(all) + countTable(attribute)(keyL2).values.map(c => -1*c*log2(c)).sum
    }
    (infoG, attribute)
  }

  //return classe più comune e controllo foglia
  def checkMostCommonClass(classValues: Vector[String], countTable: classCountLevel1): (Boolean, String) ={

    val tmp = classValues.map{
      v =>
        var counter = 0
        countTable.keys.map{
          keyL1 =>
            countTable(keyL1).keys.map{
              keyL2 =>
                counter += countTable(keyL1)(keyL2)(v)
            }
        }
        (counter,v)
    }

    //most common class label
    //classcount potrebbe essere vuota e quindi dobbiamo segnalare questa condizione
    //utilizziamo la convenzione weka di etichettare questa classe con "null"
    //secondo la teoria si potrebbe assegnare a questa foglia la classe del padre
    val classLabel = if(tmp.count(_._1 != 0) > 0) tmp.max._2 else "null"

    //controllo foglia
    val isLeaf = if(tmp.count(_._1 != 0) <= 1) true else false

    (isLeaf, classLabel)
  }

  def printTree(tree: mutable.HashMap[Int, Node], trans: Map[Int, String], values: Map[Int,Vector[String]], rootId: Int, level: Int): String ={

    val newLevel = level + 1
    var treeString = ""


    for(index <- tree(rootId).children.indices){
      if(newLevel > 1){
        treeString += "|----" * (newLevel-1)
      }

      if(tree(tree(rootId).children(index)).isLeaf){
        treeString += trans(tree(rootId).attribute) + " = " +
          values(tree(rootId).attribute)(index) + " : " +
          tree(tree(rootId).children(index)).leafLabel + "\n"
      }else{
        treeString += trans(tree(rootId).attribute) + " = " + values(tree(rootId).attribute)(index) + "\n"
        treeString += printTree(tree, trans, values, tree(rootId).children(index), newLevel)
      }
    }
    treeString
  }

  def time[R](block: => R): Double = {
    val t0 = System.nanoTime()
    val _ = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000.0 + "s")
    (t1 - t0)/1000000000.0
  }
}
