package pridwen.operators

import pridwen.types.opschema.Schema
import pridwen.types.models.{Model, Graph, JSON, Relation}

object print {
    def show_dataset[M <: Model](
        dataset: M, 
        name: String
    )(
        implicit
        schema: Schema.AsString[dataset.Schema]
    ): Unit = {
        val m: String = dataset match {
            case _: Relation => "Relation"
            case _: JSON => "JSON"
            case _: Graph => "Graph"
            case _ => "Model"
        }
        println(s"\n============= ${name}\n")
        println(s"Model: ${m}\n")
        println(s"Schema: \n${schema()}")
        if(dataset.data.size < 10) println(s"Data: ${dataset.data}\n")
        println("=======================================")
    }
}