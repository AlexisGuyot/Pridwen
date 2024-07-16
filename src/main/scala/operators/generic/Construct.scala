package pridwen.operators

import scala.collection.parallel.CollectionConverters._

import shapeless.{HList, Witness, HNil, ::}
import shapeless.labelled.{FieldType, field}

import pridwen.types.models._
import pridwen.types.opschema._

/** Nested operator to safely migrate data from a model In to a model Out.
* The operator checks that the schema of data conforms to both models.
* Some attributes can be projected to transform the schema in order to conform to the requested Out model.
*
* Relies on types Model.As, SelectField and SelectMany to ensure that the schema of data conforms and contains all the attributes requested by the user.
* The documentation only describes the parameters that cannot be infered by the compiler alone (must be specified explicitly).
*/
object construct {
    /** @param from Data to migrate from the model In to another model.
    */
    def from[In <: Model](from: In) = new {
        /** construct.from.a to migrate data to the relational or the JSON model.
        * @tparam Out The requested model (Relation or JSON).
        */
        def a[Out <: Model] = new {
            def apply[D <: Model](implicit migrate: Model.As.Aux[from.Schema, Out, D]): D = migrate(from.data)

            /** @tparam Path_To_Att HList of paths leading to the attributes to project from the schema of input data.
            */
            def withAttributes[Path_To_Att <: HList] = new {
                def apply[NewS <: HList, D <: Model](
                    implicit
                    selectAttributes: SelectMany.Aux[from.Schema, Path_To_Att, NewS],
                    migrate: Model.As.Aux[NewS, Out, D]
                ): D = migrate(from.data.par.map(tuple => selectAttributes(tuple)).toList)
            }
        }

        /** construct.from.aGraph to migrate data to the property graph model.
        * @tparam Path_To_LS The path to find the attribute containing the labels of the source nodes in the schema of input data (from).
        * @tparam Path_To_LD The path to find the attribute containing the labels of the destination nodes in the schema of input data (from).
        */
        def aGraph[Path_To_LS <: HList, Path_To_LD <: HList] = new {
            def apply[G <: Model, LS, TLS, LD, TLD](
                implicit 
                selectLS: SelectField.Aux[from.Schema, Path_To_LS, LS, TLS],
                selectLD: SelectField.Aux[from.Schema, Path_To_LD, LD, TLD],
                migrate: Model.As.Aux[FieldType[Graph.Source, FieldType[LS, TLS] :: HNil] :: FieldType[Graph.Destination, FieldType[LD, TLD] :: HNil] :: FieldType[Graph.Edge, FieldType[Witness.`'weight`.T, Int] :: HNil] :: HNil, Graph.WithID[LS, LD, Witness.`'weight`.T], G]
            ): G = time { migrate(from.data.par
                                    .groupBy(tuple => (selectLS(tuple), selectLD(tuple)))
                                    .mapValues(_.size)
                                    .map { case (key, value) => field[Graph.Source](key._1 :: HNil) :: field[Graph.Destination](key._2 :: HNil) :: field[Graph.Edge]((field[Witness.`'weight`.T](value) :: HNil)) :: HNil }
                                    .toList) }

            /** @tparam Path_To_SAtt HList of paths leading to the attributes to project into the schema of source nodes from the schema of input data.
            * @tparam Path_To_DAtt HList of paths leading to the attributes to project into the schema of destination nodes from the schema of input data.
            * @tparam Path_To_EAtt HList of paths leading to the attributes to project into the schema of edges from the schema of input data.
            */
            def withAttributes[Path_To_SAtt <: HList, Path_To_DAtt <: HList, Path_To_EAtt <: HList] = new {
                def apply[SS <: HList, DS <: HList, ES <: HList, G <: Model, LS, TLS, LD, TLD](
                    implicit
                    selectLS: SelectField.Aux[from.Schema, Path_To_LS, LS, TLS],
                    selectLD: SelectField.Aux[from.Schema, Path_To_LD, LD, TLD],
                    selectSAttr: SelectMany.Aux[from.Schema, Path_To_SAtt, SS],
                    selectDAttr: SelectMany.Aux[from.Schema, Path_To_DAtt, DS],
                    selectEAttr: SelectMany.Aux[from.Schema, Path_To_EAtt, ES],
                    migrate: Model.As.Aux[FieldType[Graph.Source, FieldType[LS, TLS] :: SS] :: FieldType[Graph.Destination, FieldType[LD, TLD] :: DS] :: FieldType[Graph.Edge, FieldType[Witness.`'weight`.T, Int] :: ES] :: HNil, Graph.WithID[LS, LD, Witness.`'weight`.T], G]
                ): G = time { migrate(from.data.par
                                    .groupBy(tuple => (selectLS(tuple) :: selectSAttr(tuple), selectLD(tuple) :: selectDAttr(tuple), selectEAttr(tuple)))
                                    .mapValues(_.size)
                                    .map { case (key, value) => field[Graph.Source](key._1) :: field[Graph.Destination](key._2) :: field[Graph.Edge]((field[Witness.`'weight`.T](value) :: key._3)) :: HNil }
                                    .toList) }
            }
        }
    }
}