import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.functions.udf

import breeze.linalg.CSCMatrix

import scala.collection.mutable.Map
import scala.collection.immutable.Queue
import scala.collection.parallel.CollectionConverters._

object sparkop {
    import commons.{time, file_grt}

    val spark = SparkSession.builder.master("local").appName("Tests Pridwen").getOrCreate
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    private val spref = "source_" ; private val dpref = "dest_" ; private val epref = "edge_"

    def load_json = {
        val df = spark.read.json("cs.json")
        (
            df.select($"user", $"retweeted_status").filter($"retweeted_status".isNotNull),
            df.select($"user", $"quoted_status").filter($"quoted_status".isNotNull)
        )
    }

    /**
    * from = a JSON as a DF.
    * slabel/dlabel = name of the attribute to use as label of the source/destination node in the new graph.
    * nlabel = name of the attribute used as node label in the new graph.
    */
    def construct_graph(from: DataFrame, slabel: String, dlabel: String, nlabel: String): DataFrame = {
        println("--- Substep 1/2: Edge aggregation.")
        val d = time { from.select(from.col(slabel).as(spref + nlabel), from.col(dlabel).as(dpref + nlabel))
              .groupBy(spref + nlabel, dpref + nlabel)
              .count()
              .withColumnRenamed("count", epref + "weight") }
        
        println("--- Substep 2/2: Modelling data as a graph.")
        time { d }
    }

    /**
    * graph = a property graph as a DF.
    * nlabel = name of the attribute used as node label in the graph.
    * community = name of the new community attribute added in the schemas of the nodes.
    */
    def detect_community(graph: DataFrame, nlabel: String, community: String): DataFrame = {
        println("--- Substep 1/3: Loading the file.")
        val g_rt = time { scala.xml.XML.loadFile(file_grt) }

        println("--- Substep 2/3: Building a node-community map.")
        val nodes = time { (g_rt \ "graph" \ "node").par.map(node => ((node \ "data").find(d => (d \@ "key") == "v_name").map(_.text).get, (node \ "data").find(d => (d \@ "key") == "v_community").map(_.text).get.toInt)).toMap }

        println("-- Substep 3/3: Creating the new graph.")
        time { val get_community = udf((id: String) => nodes.getOrElse(id, -1))
        spark.udf.register("get_community", get_community)
        graph.select(
            graph.col(spref + nlabel), get_community(graph.col(spref + nlabel)).as(spref + community),
            graph.col(dpref + nlabel), get_community(graph.col(dpref + nlabel)).as(dpref + community),
            graph.col(epref + "weight")
        )}
    }

    /**
    * graph = a property graph as a DF.
    * nlabel = name of the attribute used as node label in the graph.
    * community = name of the community attribute in the schemas of the nodes.
    */
    def keep_significant(graph: DataFrame, nlabel: String, community: String): DataFrame = {           
        println("--- Substep 1/2: Calculating the size of communities.")
        val community_sizes = time { 
            val nodes = extract_nodes(graph)
            val resolution_limit = java.lang.Math.sqrt(2*graph.count)
            println("Resolution limit: " + resolution_limit)
            nodes.groupBy(nodes.col(community)).count.filter($"count" >= resolution_limit) 
        }

        println("--- Substep 2/2: Creation of a new graph excluding members of non-significant communities")
        val new_nodes = graph.join(community_sizes, graph.col(spref + community) === community_sizes.col(community), "left_semi")
        new_nodes.join(community_sizes, new_nodes.col(dpref + community) === community_sizes.col(community), "left_semi")
    }

    /**
    * graph = a property graph as a DF.
    */
    def extract_nodes(graph: DataFrame): DataFrame = {
        val fields = graph.schema.fieldNames
        val sfields = fields.filter(_.startsWith(spref)) ; val sfields2 = sfields.map(_.replace(spref, ""))
        val dfields = fields.filter(_.startsWith(dpref)) ; val dfields2 = dfields.map(_.replace(dpref, ""))
        if(sfields2.toSet != dfields2.toSet) throw new RuntimeException(s"Nodes must have the same schema : ${sfields2.mkString(", ")} != ${dfields2.mkString(", ")}")

        sfields.foldLeft(graph)((df, f) => df.withColumnRenamed(f, f.replace(spref, "")))
            .select(sfields2.head, sfields2.tail: _*)
            .unionAll(
                dfields.foldLeft(graph)((df, f) => df.withColumnRenamed(f, f.replace(dpref, "")))
                .select(dfields2.head, dfields2.tail: _*)
            ).distinct
    }

    /**
    * graph = a graph of quotes as property graph as a DF.
    * relation = a graph of retweet's nodes as a relation as a DF.
    * gkey = name of the attribute in the schemas of the nodes to use as a key for the integration.
    * rkey = name of the attribute in the relation to use as a key for the integration.
    */
    def integrate_graphs(graph: DataFrame, relation: DataFrame, gkey: String, rkey: String): DataFrame = {
        println("--- Substep 1/2: Integrating the attributes of source nodes with the attributes of the relation.")
        val tmp = time { graph.join(relation, graph.col(spref + gkey) === relation.col(rkey), "inner")
            .withColumnRenamed("community", spref + "community")
            .drop(rkey) }

        println("--- Substep 2/2: Integrating the attributes of destination nodes with the attributes of the relation.")
        time { keep_significant(tmp.join(relation, tmp.col(dpref + gkey) === relation.col(rkey), "inner")
            .withColumnRenamed("community", dpref + "community")
            .drop(rkey), gkey, "community") }
    }

    /**
    * graph = a property graph as a DF.
    * nlabel = name of the attribute used as node label in the graph.
    * community = name of the community attribute in the schemas of the nodes.
    * weight = name of the weight attribute in the schema of the edges.
    */
    def as_matrices(graph: DataFrame, nlabel: String, community: String, weight: String) = {
        println("--- Substep 1/2: Constructing auxiliary data structures.")

        val comm_map: Map[String, Int] = Map()                  // A map associating each vertex with its community
        val rev_adj_map: Map[String, Map[String, Int]] = Map()  // A map associating each vertex with its incoming edges (adjacency matrix transpose as a map)
        
        // nodes: indexed sequence of the graph nodes
        // communities: indexed sequence of the graph communities, ordered by size (desc)
        // nodeID_to_ind: a map associating each node with its index in the adjacency (row/col) and community (row) matrices
        // rev_adj_alt: an alternate version of rev_adj_map ordered consistently with the future matrices
        val (nodes, communities, nodeID_to_ind, rev_adj_alt) = time {
            graph.select(graph.col(spref + nlabel), graph.col(spref + community), graph.col(dpref + nlabel), graph.col(dpref + community), graph.col(epref + weight)).collect.foreach(row => {
                val sid = row.getString(0) ; val did = row.getString(2)
                val scomm = row.getInt(1) ; val dcomm = row.getInt(3)
                
                comm_map(sid) = scomm ; comm_map(did) = dcomm

                rev_adj_map(did) = rev_adj_map.getOrElse(did, Map())
                rev_adj_map(did)(sid) = rev_adj_map(did).getOrElse(sid, 0) + row.getLong(4).toInt
                rev_adj_map(sid) = rev_adj_map.getOrElse(sid, Map())
            })
            
            val nodes = rev_adj_map.keys.toIndexedSeq
            val communities = comm_map.values.groupBy(x => x).toIndexedSeq.sortWith(_._2.size > _._2.size)
            val nodeID_to_ind = nodes.foldLeft((0, Map[String, Int]())){ case ((i, res), node) => (i+1, res += (node -> i)) }._2
            val rev_adj_alt = rev_adj_map.par.mapValues(m => m.toIndexedSeq.map{ case (k, v) => (nodeID_to_ind(k), v) }.sortWith((x, y) => x._1 < y._1).foldLeft(Queue[Int](), Queue[Int]()){ case ((keys, values), (k, v)) => (keys :+ k, values :+ v) })
            (nodes, communities, nodeID_to_ind, rev_adj_alt)
        }
        println(s"Significant communities: ${communities.map(x => (x._1, x._2.size)).mkString(", ")}")
        
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