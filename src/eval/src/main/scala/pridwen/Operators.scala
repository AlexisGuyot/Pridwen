import pridwen.types.models.{Model, Graph, Relation, JSON}
import pridwen.types.opschema.{SelectField, AddField, MergeSchema}
import pridwen.types.support.functions.fieldToValue

import shapeless.{HList, ::, HNil, Witness}
import shapeless.labelled.{FieldType, field}

import breeze.linalg.CSCMatrix

import org.gephi.project.api.{ProjectController, Workspace}
import org.gephi.graph.api.{GraphController, GraphModel, DirectedGraph}
import org.gephi.statistics.plugin.Modularity
import org.openide.util.Lookup

import scala.collection.mutable.Map
import scala.collection.immutable.Queue
import scala.collection.parallel.CollectionConverters._

object pridwenop {
    import commons.{time, file_grt}

    case class User(id: Double)
    case class RetweetedStatus(user: User) ; case class QuotedStatus(user: User)
    case class TweetsRT(user: User, retweeted_status: RetweetedStatus) ; case class TweetsQuotes(user: User, quoted_status: QuotedStatus)

    def load_json: (List[TweetsRT], List[TweetsQuotes]) = {
        List(os.pwd / os.RelPath("cs.json")).foldLeft((List[TweetsRT](), List[TweetsQuotes]())){(acc, file) => os.read.lines(file).par.foldLeft(acc){(datasets, line) => {
            val json = ujson.read(line)
            (
                if(json.obj.contains("retweeted_status")) TweetsRT(User(json("user")("id").str.toDouble), RetweetedStatus(User(json("retweeted_status")("user")("id").str.toDouble))) :: datasets._1 else datasets._1,
                if(json.obj.contains("quoted_status")) TweetsQuotes(User(json("user")("id").str.toDouble), QuotedStatus(User(json("quoted_status")("user")("id").str.toDouble))) :: datasets._2 else datasets._2
            )
        }}}
    }

    /**
    * S = schema of the input data.
    * slabel/SL = name of the attribute in S used as source node label in the new graph.
    * dlabel/DL = name of the attribute in S used as destination node label in the new graph.
    * nlabel/NL = name of the attribute used as label for the nodes in the new graph.
    * TL = type of the attribute used as label for the nodes in the new graph.
    */
    def construct_graph [
        ModelIn <: Model, PathToSL <: HList, PathToLD <: HList, SL, DL, TL, NL, S <: HList
    ](
        from: JSON.Aux[S], slabel: PathToSL, dlabel: PathToLD, nlabel: Witness.Aux[NL] // Essayer de passer from en Model.
    )(
        implicit
        get_slabel: SelectField.Aux[S, PathToSL, SL, TL],
        get_dlabel: SelectField.Aux[S, PathToLD, DL, TL],
        isValid: Graph.ValidSchema[
            FieldType[NL, TL] :: HNil, NL, 
            FieldType[NL, TL] :: HNil, NL, 
            FieldType[Witness.`'weight`.T, Int] :: HNil, Witness.`'weight`.T
        ]
    ): Graph.Aux[
        FieldType[NL, TL] :: HNil, 
        FieldType[NL, TL] :: HNil, 
        FieldType[Witness.`'weight`.T, Int] :: HNil, 
        NL, NL, Witness.`'weight`.T
    ] = {
        println("--- Substep 1/2: Edge aggregation.")
        val d = time { from.data.par
            .groupBy(schema => (get_slabel(schema), get_dlabel(schema)))
            .mapValues(_.size)
            .map { case (key, value) => 
                field[Graph.Source](field[NL](fieldToValue(key._1)) :: HNil) :: 
                field[Graph.Destination](field[NL](fieldToValue(key._2)) :: HNil) :: 
                field[Graph.Edge](field[Witness.`'weight`.T](value) :: HNil) :: 
                HNil 
            }.toList }

        println("--- Substep 2/2: Modelling data as a graph.")
        time { Graph[FieldType[NL, TL] :: HNil, NL, FieldType[NL, TL] :: HNil, NL, FieldType[Witness.`'weight`.T, Int] :: HNil, Witness.`'weight`.T](d) }
    }

    /**
    * SS / SL / NSS = Source node schema, label and new schema.
    * DS / DL / NDS = Destination node schema, label and new schema.
    * ES / EL = Edge schema and label.
    * GS = Graph schema (SS :: DS :: ES).
    * community/C = Name of the new community attribute.
    */
    def detect_community [
        SS <: HList, SL, NSS <: HList,
        DS <: HList, DL, NDS <: HList,
        ES <: HList, EL, GS <: HList, C <: Symbol
    ](
        graph: Graph.All[SS, DS, ES, SL, DL, EL, GS],
        community: Witness.Aux[C]
    )(
        implicit
        get_snode: SelectField.Aux[GS, Graph.Source, Graph.Source, SS],
        addIn_sschema: AddField.Aux[SS, HNil, C, String, NSS],
        get_dnode: SelectField.Aux[GS, Graph.Destination, Graph.Destination, DS],
        addIn_dschema: AddField.Aux[DS, HNil, C, String, NDS],
        isValid: Graph.ValidSchema[NSS, SL, NDS, DL, ES, EL],
        get_eattrib: SelectField.Aux[GS, Graph.Edge, Graph.Edge, ES],
        get_slabel: SelectField.Aux[GS, Graph.Source :: SL :: HNil, SL, Double],
        get_dlabel: SelectField.Aux[GS, Graph.Destination :: DL :: HNil, DL, Double],
    ): Graph.Aux[NSS, NDS, ES, SL, DL, EL] = {
        println("--- Substep 1/3: Loading the file.")
        val g_rt = time { scala.xml.XML.loadFile(file_grt) }

        println("--- Substep 2/3: Building a node-community map.")
        val nodes = time { (g_rt \ "graph" \ "node").par.map(node => ((node \ "data").find(d => (d \@ "key") == "v_name").map(_.text).get.toDouble, (node \ "data").find(d => (d \@ "key") == "v_community").map(_.text).get.toInt)).toMap }

        println("-- Substep 3/3: Creating the new graph.")
        time { Graph[NSS, SL, NDS, DL, ES, EL](
            graph.data.par.map(hlist => 
                field[Graph.Source](addIn_sschema(get_snode(hlist), s"${nodes(get_slabel(hlist))}")) :: 
                field[Graph.Destination](addIn_dschema(get_dnode(hlist), s"${nodes(get_dlabel(hlist))}")) :: 
                get_eattrib(hlist) :: HNil
            ).toList 
        )}
    }

    /**
    * Alternative version of detect_community using Gephi and the Louvain method to detect communities.
    * For validation/evaluation purposes we are using the previous version of the operator, which loads communities from a file,
    * so that we can compare the different executions with a deterministic output (the Louvain method is not deterministic). 
    *
    * SS / SL / NSS = Source node schema, label and new schema.
    * DS / DL / NDS = Destination node schema, label and new schema.
    * ES / EL = Edge schema and label.
    * GS = Graph schema (SS :: DS :: ES).
    * weight/W = Name of the weight attribute in the schema of edges.
    * community/C = Name of the new community attribute.
    */
    def detect_with_louvain [
        SS <: HList, SL, NSS <: HList,
        DS <: HList, DL, NDS <: HList,
        ES <: HList, EL, GS <: HList, 
        C <: Symbol, W <: Symbol
    ](
        graph: Graph.All[SS, DS, ES, SL, DL, EL, GS],
        weight: Witness.Aux[W],
        community: Witness.Aux[C]
    )(
        implicit
        get_snode: SelectField.Aux[GS, Graph.Source, Graph.Source, SS],
        addIn_sschema: AddField.Aux[SS, HNil, C, String, NSS],
        get_dnode: SelectField.Aux[GS, Graph.Destination, Graph.Destination, DS],
        addIn_dschema: AddField.Aux[DS, HNil, C, String, NDS],
        get_slabel: SelectField[GS, Graph.Source :: SL :: HNil],
        get_dlabel: SelectField[GS, Graph.Destination :: DL :: HNil],
        get_eattrib: SelectField.Aux[GS, Graph.Edge, Graph.Edge, ES],
        get_eweight: SelectField.Aux[GS, Graph.Edge :: W :: HNil, W, Int],
        isValid: Graph.ValidSchema[NSS, SL, NDS, DL, ES, EL]
    ): Graph.Aux[NSS, NDS, ES, SL, DL, EL] = {
        // Init Gephi Toolkit
        val pc: ProjectController = Lookup.getDefault().lookup(classOf[ProjectController])
        pc.newProject()
        val workspace: Workspace = pc.getCurrentWorkspace()
        val graphModel: GraphModel = Lookup.getDefault().lookup(classOf[GraphController]).getGraphModel()

        // Gephi Graph Construction
        val gephi_graph: DirectedGraph = graphModel.getDirectedGraph()
        graph.data.foreach(hlist => {
            val sourceID = get_slabel(hlist).toString ; val destID = get_dlabel(hlist).toString
            val source = Option(gephi_graph.getNode(sourceID)) getOrElse { val tmp = graphModel.factory().newNode(sourceID) ; gephi_graph.addNode(tmp) ; tmp }
            val dest = Option(gephi_graph.getNode(destID)) getOrElse { val tmp = graphModel.factory().newNode(destID) ; gephi_graph.addNode(tmp) ; tmp }
            val edge = graphModel.factory().newEdge(source, dest, get_eweight(hlist), true)
            gephi_graph.addEdge(edge)
        })

        // Community Detection
        val modularity: Modularity = new Modularity()
        modularity.execute(graphModel)

        val community = graphModel.getNodeTable().getColumn(Modularity.MODULARITY_CLASS)
        var community_map: Map[String, Int] = Map()
        gephi_graph.getNodes.toArray.foreach(node => community_map(node.getId.asInstanceOf[String]) = node.getAttribute(community).asInstanceOf[Int])
        
        // New Dataset Creation
        Graph[NSS, SL, NDS, DL, ES, EL](graph.data.map(hlist => 
            field[Graph.Source](addIn_sschema(get_snode(hlist), s"${community_map(get_slabel(hlist).toString)}")) :: 
            field[Graph.Destination](addIn_dschema(get_dnode(hlist), s"${community_map(get_dlabel(hlist).toString)}")) :: 
            get_eattrib(hlist) :: HNil
        ))    
    }

    /**
    * SS / SL = Source node schema and label.
    * DS / DL = Destination node schema and label.
    * ES / EL = Edge schema and label.
    * GS = Graph schema (SS :: DS :: ES).
    * community/C / TC = Name and type of the community attribute in SS and DS.
    */
    def keep_significant [
        SS <: HList, SL, DS <: HList, DL, ES <: HList, EL, 
        GS <: HList, C, TC, Out <: Model
    ](
        graph: Graph.All[SS, DS, ES, SL, DL, EL, GS],
        community: Witness.Aux[C]
    )(
        implicit
        get_snode: SelectField.Aux[GS, Graph.Source, Graph.Source, SS],
        get_dnode: SelectField.Aux[GS, Graph.Destination, Graph.Destination, DS],
        get_scomm: SelectField.Aux[SS, C, C, TC],
        get_dcomm: SelectField.Aux[DS, C, C, TC],
        same_schema: =:=[SS, DS],   // Can be used to cast terms of SS as terms of DS. 
        to_graph: Model.As.Aux[GS, Graph.WithID[SL, DL, EL], Graph.Aux[SS, DS, ES, SL, DL, EL]]
    ): Graph.Aux[SS, DS, ES, SL, DL, EL] = {
        println("--- Substep 1/2: Calculating the size of communities.")
        val community_sizes: Map[TC, Int] = Map() 
        time { graph.data.par.flatMap(tuple => List[DS](same_schema(get_snode(tuple)), get_dnode(tuple))).distinct.toList.foreach(node => { 
            val comm = get_dcomm(node)
            community_sizes(comm) = community_sizes.getOrElse(comm, 0) + 1
        }) }   

        // Creates a new graph by filtering the nodes of the input graph that are not members of a significant community (community size < resolution limit of the graph)
        println("--- Substep 2/2: Creation of a new graph excluding members of non-significant communities")
        time { 
            val resolution_limit = java.lang.Math.sqrt(2*graph.data.size)
            println("Resolution limit: " + resolution_limit)
            to_graph(graph.data.par.filter(tuple => (community_sizes(get_scomm(get_snode(tuple))) > resolution_limit) && (community_sizes(get_dcomm(get_dnode(tuple))) > resolution_limit)).toList) 
        }
    }

    /**
    * NS / NL = Node schema and label.
    * ES / EL = Edge schema and label.
    * GS = Graph schema (SS :: DS :: ES).
    * Out = Computed type for the extracted nodes with schema NS and model ModelOut.
    */
    def extract_nodes[ModelOut <: Model] = new {
        def apply [
            NS <: HList, NL, ES <: HList, EL, GS <: HList, Out <: Model
        ](
            graph: Graph.All[NS, NS, ES, NL, NL, EL, GS]
        )(
            implicit
            get_snode: SelectField.Aux[GS, Graph.Source, Graph.Source, NS],
            get_dnode: SelectField.Aux[GS, Graph.Destination, Graph.Destination, NS],
            as_modelOut: Model.As.Aux[NS, ModelOut, Out]
        ): Out = as_modelOut(graph.data.par.flatMap(tuple => List[NS](get_snode(tuple), get_dnode(tuple))).distinct.toList)
    } 

    /**
    * SS / SL / NSS = Source node schema, label and new schema.
    * DS / DL / NDS = Destination node schema, label and new schema.
    * ES / EL = Edge schema and label.
    * GS = Graph schema (SS :: DS :: ES).
    * RS = Schema of the relation.
    * gkey/GK / rkey/RK / TK = Names and type of the attributes to use as common keys for the integration.
    */
    def integrate_graphs [
        SS <: HList, SL, NSS <: HList, 
        DS <: HList, DL, NDS <: HList,
        ES <: HList, EL, GS <: HList,
        RS <: HList, GK, RK, TK
    ](
        graph: Graph.All[SS, DS, ES, SL, DL, EL, GS],
        relation: Relation.Aux[RS],
        gkey: Witness.Aux[GK],
        rkey: Witness.Aux[RK]
    )(
        implicit
        get_skey: SelectField.Aux[SS, GK, GK, TK],
        get_dkey: SelectField.Aux[DS, GK, GK, TK],
        get_rkey: SelectField.Aux[RS, GK, RK, TK],
        new_sschema: MergeSchema.Aux[SS, RS, HNil, HNil, NSS],
        new_dschema: MergeSchema.Aux[DS, RS, HNil, HNil, NDS],
        get_snode: SelectField.Aux[GS, Graph.Source, Graph.Source, SS],
        get_dnode: SelectField.Aux[GS, Graph.Destination, Graph.Destination, DS],
        get_eattrib: SelectField.Aux[GS, Graph.Edge, Graph.Edge, ES],
        isValid: Graph.ValidSchema[NSS, SL, NDS, DL, ES, EL],
    ): Graph.Aux[NSS, NDS, ES, SL, DL, EL] = {
        var rindex: Map[TK, List[RS]] = Map() ; var gindex1: Map[TK, List[graph.Schema]] = Map() ; var gindex2: Map[TK, List[(NSS, DS, ES)]] = Map()

        println("--- Substep 1/2: Integrating the attributes of source nodes with the attributes of the relation.")
        val tmp = time {
            // Creates indexes on the keys (source node label and key of the relation)
            relation.data.foreach(tuple => { rindex(get_rkey(tuple)) = tuple :: rindex.getOrElse(get_rkey(tuple), List()) })
            graph.data.foreach(edge => { gindex1(get_skey(get_snode(edge))) = edge :: gindex1.getOrElse(get_skey(get_snode(edge)), List()) })

            // Joins the schema of source nodes with the schema of the relation
            rindex.keySet.intersect(gindex1.keySet).par.flatMap(key => 
                rindex(key).flatMap(tuple => gindex1(key).foldLeft(List[(NSS, DS, ES)]()){(acc, edge) => 
                    if(get_rkey(tuple) == get_skey(get_snode(edge))) (new_sschema(get_snode(edge), tuple), fieldToValue(get_dnode(edge)), fieldToValue(get_eattrib(edge))) :: acc else acc
                })
            ).toList
        }

        println("--- Substep 2/2: Integrating the attributes of destination nodes with the attributes of the relation.")
        time {
            // Creates an index on the destination node label
            tmp.foreach(edge => { gindex2(get_dkey(edge._2)) = edge :: gindex2.getOrElse(get_dkey(edge._2), List()) })

            // Joins the schema of destination nodes with the schema of the relation
            Graph[NSS, SL, NDS, DL, ES, EL](rindex.keySet.intersect(gindex2.keySet).par.flatMap(key => 
                rindex(key).flatMap(tuple => gindex2(key).foldLeft(List[FieldType[Graph.Source, NSS] :: FieldType[Graph.Destination, NDS] :: FieldType[Graph.Edge, ES] :: HNil]()){(acc, edge) => 
                    if(get_rkey(tuple) == get_dkey(edge._2)) (field[Graph.Source](edge._1) :: field[Graph.Destination](new_dschema(edge._2, tuple)) :: field[Graph.Edge](edge._3) :: HNil) :: acc else acc
                })
            ).toList)
        }
    }

    /**
    * SS / SL / DS / DL / ES / EL = Source/Destination node or Edge schema and label.
    * GS = Graph schema (SS :: DS :: ES).
    * community/C / TC / weight/W / TL = Name and/or type of the community/weight/label attribute in SS/DS/EL.
    */
    def as_matrices [
        SS <: HList, SL, DS <: HList, DL, ES <: HList, EL, 
        GS <: HList, C, W, TC, TL
    ](
        graph: Graph.All[SS, DS, ES, SL, DL, EL, GS],
        community: Witness.Aux[C],
        weight: Witness.Aux[W]
    )(
        implicit
        get_scomm: SelectField.Aux[GS, Graph.Source :: C :: HNil, C, TC],
        get_dcomm: SelectField.Aux[GS, Graph.Destination :: C :: HNil, C, TC],
        get_eweight: SelectField.Aux[GS, Graph.Edge :: W :: HNil, W, Int],
        get_slabel: SelectField.Aux[GS, Graph.Source :: SL :: HNil, SL, TL],
        get_dlabel: SelectField.Aux[GS, Graph.Destination :: DL :: HNil, DL, TL],
    ): (CSCMatrix[Int], CSCMatrix[Int]) = {
        println("--- Substep 1/2: Constructing auxiliary data structures.")

        val comm_map: Map[TL, TC] = Map()               // A map associating each vertex with its community
        val rev_adj_map: Map[TL, Map[TL, Int]] = Map()  // A map associating each vertex with its incoming edges (adjacency matrix transpose as a map)
        
        // nodes: indexed sequence of the graph nodes
        // communities: indexed sequence of the graph communities, ordered by size (desc)
        // nodeID_to_ind: a map associating each node with its index in the adjacency (row/col) and community (row) matrices
        // rev_adj_alt: an alternate version of rev_adj_map ordered consistently with the future matrices
        val (nodes, communities, nodeID_to_ind, rev_adj_alt) = time {
            graph.data.foreach(edge => {
                val sid = get_slabel(edge) ; val did = get_dlabel(edge)
                val scomm = get_scomm(edge); val dcomm = get_dcomm(edge)
                
                comm_map(sid) = scomm ; comm_map(did) = dcomm

                rev_adj_map(did) = rev_adj_map.getOrElse(did, Map())
                rev_adj_map(did)(sid) = rev_adj_map(did).getOrElse(sid, 0) + get_eweight(edge)
                rev_adj_map(sid) = rev_adj_map.getOrElse(sid, Map())
            })
            
            val nodes = rev_adj_map.keys.toIndexedSeq
            val communities = comm_map.values.groupBy(x => x).toIndexedSeq.sortWith(_._2.size > _._2.size)
            val nodeID_to_ind = nodes.foldLeft((0, Map[TL, Int]())){ case ((i, res), node) => (i+1, res += (node -> i)) }._2
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