package id3

import io.Source
import scala.collection.mutable

object ID3 {
  //type alias
  type classCountLevel1 = mutable.HashMap[Int, mutable.HashMap[String, mutable.HashMap[String, Int]]]
  type classCountLevel2 = mutable.HashMap[String, mutable.HashMap[String, Int]]
  type classCountLevel3 = mutable.HashMap[String, Int]

  def main(args: Array[String]): Unit ={

    //setup data tennis
    val attributes = (0 until 4).toSet
    val targetAttribute = 4
    val translateAttr = Map(
      (0, "Outlook"),
      (1, "Temperature"),
      (2, "Humidity"),
      (3, "Wind")
    )
    val values = Map(
      (0, Vector("Sunny", "Overcast", "Rain")),   //outlook = 0
      (1, Vector("Hot", "Mild", "Cool")),         //temperature = 1
      (2, Vector("High", "Normal")),              //humidity = 2
      (3, Vector("Weak", "Strong")),              //wind = 3
      //(4, Vector("Yes", "No"))                  //class = 4  alcune versioni hanno Yes altre Si
      (4, Vector("Si","No"))
    )

    /*setup data car
    val attributes = (0 until 6).toSet
    val targetAttribute = 6
    val translateAttr = Map(
      (0, "buying"),
      (1, "maint"),
      (2, "doors"),
      (3, "persons"),
      (4, "lug_boot"),
      (5, "safety")
    )
    val values = Map(
      (0, Vector("vhigh", "high", "med","low")),   //buying = 0
      (1, Vector("vhigh", "high", "med","low")),   //maint = 1
      (2, Vector("2","3","4","5more")),            //doors = 2
      (3, Vector("2","4","more")),                 //persons = 3
      (4, Vector("small","med","big")),            //lug_boot = 4
      (5, Vector("low","med","high")),             //safety = 5
      (6, Vector("unacc","acc","good","vgood"))    //class = 6
    ) */


    //file setup
    val path = "/home/luigi/data/TEST_UFFICIALI/tennis3MB.txt"  //args(0)
    var source = Source.fromFile(path)

    //algorithm setup
    val tree = mutable.HashMap.empty[Int, Node]
    //create root
    val root = new Node(id=0, -1, attributes)
    //add root
    tree(0) = root
    var nodesToProcess = List[Node](root)

    time{
      while(nodesToProcess.nonEmpty){
        //init
        val data = source.getLines().map(row => row.split(","))

        //setup
        val partialClassCounts = new mutable.HashMap[Int, classCountLevel1]()
        nodesToProcess.foreach{node => setup(node,partialClassCounts,attributes,values,targetAttribute)}

        //process data
        data.foreach{row => processRow(row, partialClassCounts, tree, values, targetAttribute)}

        //tmp buffer
        var nodesToAdd = List.empty[Node]

        partialClassCounts.foreach{
          case (id, countTable) =>
            nodesToAdd = processTable(id, countTable, tree, values, targetAttribute) ::: nodesToAdd
        }

        nodesToProcess = nodesToAdd
        //reset file
        source = Source.fromFile(path)
      }
    }

    //close file
    source.close()

    //println(args(0))
    println("\n---new tree---\n")
    printTree(tree, translateAttr, values, 0, 0)
    println("\n---end tree---\n")
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
      foreach(attr => {
        partialClassCounts(node.id)(attr) = new classCountLevel2()
        values(attr)
          .foreach(va => {
            partialClassCounts(node.id)(attr)(va) = new classCountLevel3()
            values(targetAttribute)
              .foreach(vc => partialClassCounts(node.id)(attr)(va)(vc) = 0)
          })
      })
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
      //UNUSED ATTRIBUTE PUÒ ESSERE VUOTO!
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
      //dobbiamo creare una foglia
      val (isLeaf, mostCommonClassLabel) = checkMostCommonClass(values(targetAttribute),countTable)
      //aggiorno albero
      tree(id).setIsleaf(true)
      //se la countTable contiene solo zero, allora bisogna prendere la classe del padre
      tree(id).setLeafLabel(mostCommonClassLabel)
    }else{
      val aBest = tree(id).unusedAttributes.map(attr => infoGain(attr, countTable)).min._2
      //ora bisogna aggiornare l'albero con i rispettivi aBest
      tree(id).setAttribute(aBest)
      //ora devo controllare se è una foglia e prendere la classe più comune
      val (isLeaf, mostCommonClassLabel) = checkMostCommonClass(values(targetAttribute),countTable)
      //aggiorno albero
      tree(id).setIsleaf(isLeaf)
      tree(id).setLeafLabel(mostCommonClassLabel)

      //se non è una foglia devo creare ed aggiornare i figli
      //un figlio per ogni valore di abest
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
    //condizione terminazione visita
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

  def printTree(tree: mutable.HashMap[Int, Node], trans: Map[Int, String], values: Map[Int,Vector[String]], rootId: Int, level: Int): Unit ={

    //new print: weka tree style
    var newLevel = level + 1

    for(index <- tree(rootId).children.indices){
      if(newLevel > 1){
        print("|----" * (newLevel-1))
      }

      if(tree(tree(rootId).children(index)).isLeaf){
        println(s"${trans(tree(rootId).attribute)} " +
          s"= ${values(tree(rootId).attribute)(index)}" +
          s": ${tree(tree(rootId).children(index)).leafLabel}")
      }else{
        println(s"${trans(tree(rootId).attribute)} = ${values(tree(rootId).attribute)(index)}")
        printTree(tree, trans, values, tree(rootId).children(index), newLevel)
      }
    }
  }

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0)/1000000000.0 + "s")
    result
  }
}
