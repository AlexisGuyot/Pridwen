import commons._

import pridwenop._
import pridwen.types.models.{JSON, Relation}
import shapeless.{::, HNil, Witness}

object WorkflowPridwen extends App {  
    println("\n======= Data loading =======")

    val (input_dataset_rt, input_dataset_q) = time { 
        val (data_rt, data_q) = load_json
        (JSON(data_rt), JSON(data_q)) 
    }

    println("\n======= Workflow execution =======")

    println("-- Step 1/8: Building the graph of retweets.")
    val g_rt = time { construct_graph(
        input_dataset_rt, 
        Witness('user) :: Witness('id) :: HNil,
        Witness('retweeted_status) :: Witness('user) :: Witness('id) :: HNil,
        Witness('uid)
    ) }
    /* val g_rt = time { construct_graph(
        input_dataset_rt, 
        Witness('user) :: HNil,
        Witness('retweeted_status) :: Witness('user) :: HNil,
        Witness('uid)
    ) } */                                                                                      // Bad transfo


    println("\n-- Step 2/8: Community detection.")
    val g_rt2 = time { detect_community(g_rt, Witness('community)) }

    println("\n-- Step 3/8: Elimination of non-significant communities.")
    val g_rt3 = time { keep_significant(g_rt2, Witness('community)) }
    //val g_rt3 = time { keep_significant(g_rt2, Witness('comunity)) }                          // No att

    println("\n-- Step 4/8: Extracting the nodes of the graph of retweets.")
    val n_rt = time { extract_nodes[Relation](g_rt3) }

    println("\n-- Step 5/8: Building the graph of quotes.")
    val g_q = time { construct_graph(
        input_dataset_q, 
        Witness('user) :: Witness('id) :: HNil,
        Witness('quoted_status) :: Witness('user) :: Witness('id) :: HNil,
        Witness('uid)
    ) }

    println("\n-- Step 6/8: Integrating the graph of quotes with the nodes of the graph of retweets.")
    val g_i = time { integrate_graphs(g_q, n_rt, Witness('uid), Witness('uid)) }
    //val g_i = time { integrate_graphs(g_q, g_rt3, Witness('uid), Witness('uid)) }              // Bad model

    println("\n-- Step 7/8: Creating the adjacency and community matrices.")
    val (m_adj, m_com) = time { as_matrices(g_i, Witness('community), Witness('weight)) }
    //val (m_adj, m_com) = time { as_matrices(g_i, Witness('weight), Witness('community)) }     // Bad att

    println("\n-- Step 8/8: Calculating polarisation measurements.")
    val (m_ant, m_por) = time { eval_polarisation(m_adj, m_com) }

    println("\n======= Polarisation metrics =======")

    println("- Antagonism matrix:")
    println(m_ant)
    println("- Porosity matrix:")
    println(m_por)

    println("====================================")
}
