
import breeze.linalg.{CSCMatrix, sum, Axis}

import scala.collection.mutable.Map
import scala.collection.immutable.Queue
import scala.collection.parallel.CollectionConverters._

object control {
    import commons.{time, file_grt}

    def load_json: (List[(Double, Double)], List[(Double, Double)]) = {
        List(os.pwd / os.RelPath("cs.json")).foldLeft((List[(Double, Double)](), List[(Double, Double)]())){(acc, file) => os.read.lines(file).par.foldLeft(acc){(datasets, line) => {
            val json = ujson.read(line)
            (
                if(json.obj.contains("retweeted_status")) (json("user")("id").str.toDouble, json("retweeted_status")("user")("id").str.toDouble) :: datasets._1 else datasets._1,
                if(json.obj.contains("quoted_status")) (json("user")("id").str.toDouble, json("quoted_status")("user")("id").str.toDouble) :: datasets._2 else datasets._2
            )
        }}}
    }

    /**
    * from = user.id-retweeted/quoted_status.user.id pairs used as node labels.
    */
    def construct_graph(from: List[(Double, Double)]) = {
        println("--- Substep 1/2: Edge aggregation.")
        val d = time { from.par
            .groupBy(tuple => (tuple._1, tuple._2))
            .mapValues(_.size)
            .map { case (key, value) => (key._1, key._2, value) }
            .toList }

        println("--- Substep 2/2: Modelling data as a graph.")
        time { d }
    }

    /**
    * graph = graph as a list of edges (slabel, dlabel, weight).
    */
    def detect_community(graph: List[(Double, Double, Int)]): List[((Double, Int), (Double, Int), Int)] = {
        println("--- Substep 1/3: Loading the file.")
        val g_rt = time { scala.xml.XML.loadFile(file_grt) }

        println("--- Substep 2/3: Building a node-community map.")
        val nodes = time { (g_rt \ "graph" \ "node").par.map(node => ((node \ "data").find(d => (d \@ "key") == "v_name").map(_.text).get.toDouble, (node \ "data").find(d => (d \@ "key") == "v_community").map(_.text).get.toInt)).toMap }

        println("-- Substep 3/3: Creating the new graph.")
        time { graph.par.map(tuple => ((tuple._1, nodes(tuple._1)), (tuple._2, nodes(tuple._2)), tuple._3)).toList }
    }

    /**
    * graph = property graph as a list of edges ((slabel, scomm), (dlabel, dcomm), weight).
    */
    def keep_significant(
        graph: List[((Double, Int), (Double, Int), Int)]
    ): List[((Double, Int), (Double, Int), Int)] = {
        println("--- Substep 1/2: Calculating the size of communities.")
        val community_sizes: Map[Int, Int] = Map() 
        time { extract_nodes(graph).foreach(node => { 
            val comm = node._2
            community_sizes(comm) = community_sizes.getOrElse(comm, 0) + 1
        }) }       

        // Creates a new graph by filtering the nodes of the input graph that are not members of a significant community (community size < resolution limit of the graph)
        println("--- Substep 2/2: Creation of a new graph excluding members of non-significant communities")
        time { 
            val resolution_limit = java.lang.Math.sqrt(2*graph.size)
            println("Resolution limit: " + resolution_limit)
            graph.par.filter(tuple => (community_sizes(tuple._1._2) > resolution_limit) && (community_sizes(tuple._2._2) > resolution_limit)).toList 
        }
    }

    /**
    * graph = property graph as a list of edges ((slabel, scomm), (dlabel, dcomm), weight).
    */
    def extract_nodes(graph: List[((Double, Int), (Double, Int), Int)]): List[(Double, Int)] 
        = graph.par.flatMap(tuple => List[(Double, Int)](tuple._1, tuple._2)).distinct.toList

    /**
    * graph = graph of quotes as a property graph as a list of edges (slabel, dlabel, weight).
    * relation = nodes of a graph of retweets as a relation as a list of tuples (nlabel, ncomm).
    */
    def integrate_graphs(
        graph: List[(Double, Double, Int)], relation: List[(Double, Int)]
    ): List[((Double, Int), (Double, Int), Int)] = {
        var lindex: Map[Double, List[(Double, Int)]] = Map() ; var rindex1: Map[Double, List[(Double, Double, Int)]] = Map() ; var rindex2: Map[Double, List[((Double, Int), Double, Int)]] = Map()
        
        println("--- Substep 1/2: Integrating the attributes of source nodes with the attributes of the relation.")
        val tmp = time {
            relation.foreach(tuple => { lindex(tuple._1) = tuple :: lindex.getOrElse(tuple._1, List()) })
            graph.foreach(tuple => { rindex1(tuple._1) = tuple :: rindex1.getOrElse(tuple._1, List()) })
            lindex.keySet.intersect(rindex1.keySet).par.flatMap(key => lindex(key).flatMap(ltuple => rindex1(key).foldLeft(List[((Double, Int), Double, Int)]()){(acc, rtuple) => if(ltuple._1 == rtuple._1) (ltuple, rtuple._2, rtuple._3) :: acc else acc})).toList
        }

        println("--- Substep 2/2: Integrating the attributes of destination nodes with the attributes of the relation.")
        time {
            tmp.foreach(tuple => { rindex2(tuple._2) = tuple :: rindex2.getOrElse(tuple._2, List()) })
            lindex.keySet.intersect(rindex2.keySet).par.flatMap(key => lindex(key).flatMap(ltuple => rindex2(key).foldLeft(List[((Double, Int), (Double, Int), Int)]()){(acc, rtuple) => if(ltuple._1 == rtuple._2) (rtuple._1, ltuple, rtuple._3) :: acc else acc})).toList
        }
    }

    /**
    * graph = property graph as a list of edges ((slabel, scomm), (dlabel, dcomm), weight).
    */
    def as_matrices(
        graph: List[((Double, Int), (Double, Int), Int)]
    ): (CSCMatrix[Int], CSCMatrix[Int]) = {
        println("--- Substep 1/2: Constructing auxiliary data structures.")

        val comm_map: Map[Double, Int] = Map()                  // A map associating each vertex with its community
        val rev_adj_map: Map[Double, Map[Double, Int]] = Map()  // A map associating each vertex with its incoming edges (adjacency matrix transpose as a map)
        
        // nodes: indexed sequence of the graph nodes
        // communities: indexed sequence of the graph communities, ordered by size (desc)
        // nodeID_to_ind: a map associating each node with its index in the adjacency (row/col) and community (row) matrices
        // rev_adj_alt: an alternate version of rev_adj_map ordered consistently with the future matrices
        val (nodes, communities, nodeID_to_ind, rev_adj_alt) = time {
            graph.foreach(tuple => {
                val sid = tuple._1._1 ; val did = tuple._2._1
                val scomm = tuple._1._2 ; val dcomm = tuple._2._2
                
                comm_map(sid) = scomm ; comm_map(did) = dcomm

                rev_adj_map(did) = rev_adj_map.getOrElse(did, Map())
                rev_adj_map(did)(sid) = rev_adj_map(did).getOrElse(sid, 0) + tuple._3
                rev_adj_map(sid) = rev_adj_map.getOrElse(sid, Map())
            })

            val nodes = rev_adj_map.keys.toIndexedSeq
            val communities = comm_map.values.groupBy(x => x).toIndexedSeq.sortWith(_._2.size > _._2.size)
            val nodeID_to_ind = nodes.foldLeft((0, Map[Double, Int]())){ case ((i, res), node) => (i+1, res += (node -> i)) }._2
            val rev_adj_alt = rev_adj_map.par.mapValues(m => m.toIndexedSeq.map{ case (k, v) => (nodeID_to_ind(k), v) }.sortWith((x, y) => x._1 < y._1).foldLeft(Queue[Int](), Queue[Int]()){ case ((keys, values), (k, v)) => (keys :+ k, values :+ v) })
            (nodes, communities, nodeID_to_ind, rev_adj_alt)
        }
        println(s"Significant communities: ${communities.map(x => (x._1, x._2.size)).mkString(" ; ")}")
        
        
        println("--- Substep 2/2: Constructing the adjacency and community matrices.")
        time {
            val nb_nodes = nodes.length ; val nb_comm = communities.length

            // Computes the adjacency and community matrices by building the auxiliary arrays forming the CSC Matrices
            // Auxiliary arrays for the adjacency matrix
            val (data, indices, indptr, _) = nodes.foldLeft((Queue[Int](), Queue[Int](), Queue[Int](), 0)){ case ((d, ind, iptr, i), n) => {
                val rev_adj_n = rev_adj_alt(n)
                (d :++ rev_adj_n._2, ind :++ rev_adj_n._1, iptr :+ i, i + rev_adj_n._1.size )
            }}

            // Auxiliary arrays for the community matrix
            val (indicesc, indptrc, _) = comm_map.groupMap(_._2)(_._1).toSeq.sortWith(_._2.size > _._2.size).foldLeft((Queue[Int](), Queue[Int](), 0)){case ((ind, iptr, i), (community, members)) => 
                (ind :++ members.map(m => nodeID_to_ind(m)), iptr :+ i, i + members.size )
            }

            (
                new CSCMatrix(data.toArray, nb_nodes, nb_nodes, (indptr :+ data.size).toArray, indices.toArray),                // Adjacency matrix
                new CSCMatrix(Array.fill(nb_nodes){1}, nb_nodes, nb_comm, (indptrc :+ nb_nodes).toArray, indicesc.toArray)      // Community matrix
            )
        }
    }
}