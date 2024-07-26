import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.{StructType, StructField}
import org.apache.spark.sql.functions.{udf, col}

import breeze.linalg.CSCMatrix

import scala.collection.mutable.Map
import scala.collection.immutable.Queue
import scala.collection.parallel.CollectionConverters._

import commons._

object WorkflowDataset extends App {  
    val spark = SparkSession.builder.master("local").appName("Tests Pridwen").getOrCreate
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    println("\n======= Data loading =======")

    val (input_dataset_rt, input_dataset_q) = time { 
        val df = spark.read.json("cs.json")
        (
            df.select($"user", $"retweeted_status").filter($"retweeted_status".isNotNull),
            df.select($"user", $"quoted_status").filter($"quoted_status".isNotNull)
        )
    }

    println("\n======= Workflow execution =======")

    println("-- Step 1/8: Building the graph of retweets.")

    case class NodeRT1(uid: String) ; case class Edge(weight: Int)
    case class SchemaRT1(source: NodeRT1, dest: NodeRT1, edge: Edge)
    case class BadNodeRT1(uid: List[String]) ; case class BadSchemaRT1(source: BadNodeRT1, dest: BadNodeRT1, edge: Edge)
    case class TmpSchema(source: String, dest: String)

    val g_rt = time { 
        println("--- Substep 1/2: Edge aggregation.")
        val d = time { input_dataset_rt.select(input_dataset_rt.col("user.id").as("source"), input_dataset_rt.col("retweeted_status.user.id").as("dest"))
              .as[TmpSchema]
              .groupByKey(tuple => (tuple.source, tuple.dest))
              .count() }
        
        println("--- Substep 2/2: Modelling data as a graph.")
        time { d.map(tuple => SchemaRT1(NodeRT1(tuple._1._1), NodeRT1(tuple._1._2), Edge(tuple._2.toInt))).as[SchemaRT1] }   
        //time { d.map(tuple => BadSchemaRT1(BadNodeRT1(List(tuple._1._1)), BadNodeRT1(List(tuple._1._2)), Edge(tuple._2.toInt))).as[BadSchemaRT1] }      // Bad transfo (1/2)     
    }

    println("\n-- Step 2/8: Community detection.")

    case class NodeRT2(uid: String, community: Int)
    case class SchemaRT2(source: NodeRT2, dest: NodeRT2, edge: Edge)
    val g_rt2: Dataset[SchemaRT2] = time { 
        println("--- Substep 1/3: Loading the file.")
        val xml = time { scala.xml.XML.loadFile(file_grt) }

        println("--- Substep 2/3: Building a node-community map.")
        val nodes = time { (xml \ "graph" \ "node").par.map(node => ((node \ "data").find(d => (d \@ "key") == "v_name").map(_.text).get, (node \ "data").find(d => (d \@ "key") == "v_community").map(_.text).get.toInt)).toMap }

        println("-- Substep 3/3: Creating the new graph.")
        time { val get_community = udf((uid: String) => nodes.getOrElse(uid, -1))
        spark.udf.register("get_community", get_community)
        g_rt.map(tuple => SchemaRT2(
            NodeRT2(tuple.source.uid, nodes.getOrElse(tuple.source.uid, -1)),
            NodeRT2(tuple.dest.uid, nodes.getOrElse(tuple.dest.uid, -1)),
            Edge(tuple.edge.weight)
        ))}                
        /* g_rt.map(tuple => SchemaRT2(
            NodeRT2(tuple.source.uid(0), nodes.getOrElse(tuple.source.uid(0), -1)),
            NodeRT2(tuple.dest.uid(0), nodes.getOrElse(tuple.dest.uid(0), -1)),
            Edge(tuple.edge.weight)
        ))} */                                                                                                                                            // Bad transfo (2/2)                                                                                                                          
    }

    println("\n-- Step 3/8: Elimination of non-significant communities.")
    val g_rt3: Dataset[SchemaRT2] = time { 
        println("--- Substep 1/2: Calculating the size of communities.")
        val community_sizes = time { 
            val nodes: Dataset[NodeRT2] = g_rt2.map(tuple => tuple.source).distinct.union(g_rt2.map(tuple => tuple.dest)).distinct
            val resolution_limit = java.lang.Math.sqrt(2*g_rt2.count)
            println("Resolution limit: " + resolution_limit)
            nodes.groupByKey(node => node.community).count.filter(pair => pair._2 >= resolution_limit).withColumnRenamed("key", "community") 
            //nodes.groupByKey(node => node.comunity).count.filter(pair => pair._2 >= resolution_limit).withColumnRenamed("key", "community")           // No att
        }

        println("--- Substep 2/2: Creation of a new graph excluding members of non-significant communities")
        val new_nodes = g_rt2.join(community_sizes, g_rt2.col("source.community") === community_sizes.col("community"), "left_semi")
        new_nodes.join(community_sizes, new_nodes.col("dest.community") === community_sizes.col("community"), "left_semi").as[SchemaRT2]
    }

    println("\n-- Step 4/8: Extracting the nodes of the graph of retweets.")
    val n_rt: Dataset[NodeRT2] = time { 
        g_rt3.map(tuple => tuple.source).distinct.union(g_rt3.map(tuple => tuple.dest)).distinct
    }

    println("\n-- Step 5/8: Building the graph of quotes.")
    case class NodeQ1(uid: String) 
    case class SchemaQ1(source: NodeQ1, dest: NodeQ1, edge: Edge)
    val g_q: Dataset[SchemaQ1] = time { 
        println("--- Substep 1/2: Edge aggregation.")
        val d = time { input_dataset_q.select(input_dataset_q.col("user.id").as("source"), input_dataset_q.col("quoted_status.user.id").as("dest"))
              .as[TmpSchema]
              .groupByKey(tuple => (tuple.source, tuple.dest))
              .count() }
        
        println("--- Substep 2/2: Modelling data as a graph.")
        time { d.map(tuple => SchemaQ1(NodeQ1(tuple._1._1), NodeQ1(tuple._1._2), Edge(tuple._2.toInt))).as[SchemaQ1] }
    }

    println("\n-- Step 6/8: Integrating the graph of quotes with the nodes of the graph of retweets.")

    case class NodeQ2(uid: String, community: Int) 
    case class SchemaQ2(source: NodeQ2, dest: NodeQ2, edge: Edge)
    case class TmpSchemaQ2(source: NodeQ2, dest: NodeQ1, edge: Edge)
    val g_i: Dataset[SchemaQ2] = time { 
        println("--- Substep 1/2: Integrating the attributes of source nodes with the attributes of the relation.")
        val graph = g_q ; val relation = n_rt
        //val graph = g_q ; val relation = g_rt3                                                                                                         // Bad model
        val tmp = time { graph.join(relation, graph.col("source.uid") === relation.col("uid"), "inner").withColumnRenamed("community", "source_community").drop("uid") }

        println("--- Substep 2/2: Integrating the attributes of destination nodes with the attributes of the relation.")
        val new_graph: Dataset[SchemaQ2] = time { 
            tmp.join(relation, tmp.col("dest.uid") === relation.col("uid"), "inner")
                .select(col("source.uid"), col("source_community"), col("dest.uid"), col("community"), col("edge.weight"))
                .map(row => SchemaQ2(NodeQ2(row.getString(0), row.getInt(1)), NodeQ2(row.getString(2), row.getInt(3)), Edge(row.getInt(4)))) 
        }
        /* val tmp = time { graph.joinWith(relation, graph.col("source.uid") === relation.col("uid"), "inner")
                            .map(tuple => TmpSchemaQ2(NodeQ2(tuple._1.source.uid, tuple._2.community), tuple._1.dest, tuple._1.edge))
         }

        println("--- Substep 2/2: Integrating the attributes of destination nodes with the attributes of the relation.")
        val new_graph: Dataset[SchemaQ2] = time { tmp.joinWith(relation, tmp.col("dest.uid") === relation.col("uid"), "inner")
            .map(tuple => SchemaQ2(tuple._1.source, NodeQ2(tuple._1.dest.uid, tuple._2.community), tuple._1.edge)) } */

        println("--- Substep 1/2: Calculating the size of communities.")
        val community_sizes = time { 
            val nodes: Dataset[NodeQ2] = new_graph.map(tuple => tuple.source).distinct.union(new_graph.map(tuple => tuple.dest)).distinct
            val resolution_limit = java.lang.Math.sqrt(2*new_graph.count)
            println("Resolution limit: " + resolution_limit)
            nodes.groupByKey(node => node.community).count.filter(pair => pair._2 >= resolution_limit).withColumnRenamed("key", "community") 
        }

        println("--- Substep 2/2: Creation of a new graph excluding members of non-significant communities")
        val new_nodes = new_graph.join(community_sizes, new_graph.col("source.community") === community_sizes.col("community"), "left_semi")
        new_nodes.join(community_sizes, new_nodes.col("dest.community") === community_sizes.col("community"), "left_semi").as[SchemaQ2]
    }

    println("\n-- Step 7/8: Creating the adjacency and community matrices.")
    val (m_adj, m_com) = time { 
        println("--- Substep 1/2: Constructing auxiliary data structures.")

        val comm_map: Map[String, Int] = Map()                  // A map associating each vertex with its community
        val rev_adj_map: Map[String, Map[String, Int]] = Map()  // A map associating each vertex with its incoming edges (adjacency matrix transpose as a map)
        
        // nodes: indexed sequence of the graph nodes
        // communities: indexed sequence of the graph communities, ordered by size (desc)
        // nodeID_to_ind: a map associating each node with its index in the adjacency (row/col) and community (row) matrices
        // rev_adj_alt: an alternate version of rev_adj_map ordered consistently with the future matrices
        val (nodes, communities, nodeID_to_ind, rev_adj_alt) = time {
            g_i.collect.foreach(tuple => {
                val sid = tuple.source.uid ; val did = tuple.dest.uid
                val scomm = tuple.source.community ; val dcomm = tuple.dest.community
                /* val sid = tuple.source.community ; val did = tuple.dest.community
                val scomm = tuple.source.uid ; val dcomm = tuple.dest.uid */                                                                            // Bad att
                
                comm_map(sid) = scomm ; comm_map(did) = dcomm

                rev_adj_map(did) = rev_adj_map.getOrElse(did, Map())
                rev_adj_map(did)(sid) = rev_adj_map(did).getOrElse(sid, 0) + tuple.edge.weight
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

    println("\n-- Step 8/8: Calculating polarisation measurements.")
    val (m_ant, m_por) = time { 
        eval_polarisation(m_adj, m_com) 
    }

    println("\n======= Polarisation metrics =======")

    println("- Antagonism matrix:")
    println(m_ant)
    println("\n- Porosity matrix:")
    println(m_por)

    println("====================================")

    spark.close
}
