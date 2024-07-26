import commons._

import dframeop._

object WorkflowDataFrame extends App {  
    println("\n======= Data loading =======")

    val (input_dataset_rt, input_dataset_q) = time { load_json }

    println("\n======= Workflow execution =======")

    println("-- Step 1/8: Building the graph of retweets.")
    val g_rt = time { construct_graph(input_dataset_rt, "user.id", "retweeted_status.user.id", "uid") }
    //val g_rt = time { construct_graph(input_dataset_rt, "user", "retweeted_status.user", "uid") }     // Bad transfo

    println("\n-- Step 2/8: Community detection.")
    val g_rt2 = time { detect_community(g_rt, "uid", "community") }

    println("\n-- Step 3/8: Elimination of non-significant communities.")
    val g_rt3 = time { keep_significant(g_rt2, "uid", "community") }
    //val g_rt3 = time { keep_significant(g_rt2, "uid", "comunity") }                                   // No att

    println("\n-- Step 4/8: Extracting the nodes of the graph of retweets.")
    val n_rt = time { extract_nodes(g_rt3) }

    println("\n-- Step 5/8: Building the graph of quotes.")
    val g_q = time { construct_graph(input_dataset_q, "user.id", "quoted_status.user.id", "uid") }

    println("\n-- Step 6/8: Integrating the graph of quotes with the nodes of the graph of retweets.")
    val g_i = time { integrate_graphs(g_q, n_rt, "uid", "uid") }
    //val g_i = time { integrate_graphs(g_q, g_rt3, "uid", "uid") }                                     // Bad model

    println("\n-- Step 7/8: Creating the adjacency and community matrices.")
    val (m_adj, m_com) = time { as_matrices(g_i, "uid", "community", "weight") }
    //val (m_adj, m_com) = time { as_matrices(g_i, "uid", "weight", "community") }                      // Bad att

    println("\n-- Step 8/8: Calculating polarisation measurements.")
    val (m_ant, m_por) = time { eval_polarisation(m_adj, m_com) }

    println("\n======= Polarisation metrics =======")

    println("- Antagonism matrix:")
    println(m_ant)
    println("\n- Porosity matrix:")
    println(m_por)

    println("====================================")

    spark.close
}
