package pridwen.dataset

import pridwen.types.opschema.SelectField
import pridwen.types.support.DecompPath

import shapeless.{HList, ::, HNil, Witness}
import shapeless.labelled.FieldType
import shapeless.ops.hlist.{Selector, Prepend}

import org.apache.spark.sql.Column

trait AggOps[O, I]
object AggOps {
    import ColumnOps._

    trait And[O1 <: AggOperator, O2 <: AggOperator] extends AggOperator 

    trait Compute[S <: HList, O <: AggOperator, I] { type AddTo <: HList ; type NewF <: HList ; def toSparkColumn: List[Column] ; def newNames: List[(String, String)] }
    object Compute {
        type Aux[S <: HList, O <: AggOperator, I, AddTo0 <: HList, NewF0 <: HList] = Compute[S, O, I] { type AddTo = AddTo0 ; type NewF = NewF0 }

        implicit def second_input_is_path[
            S <: HList, WP <: HList, SP <: HList, P <: HList, I2, FN <: Symbol, FT, O <: AggOperator, NGS <: HList, NFT, NFN <: Symbol, NS <: HList
        ](
            implicit
            select_f: SelectField.Aux[S, SP, FN, FT],
            validArgs: AggOpOut.Aux[O, FT, NFN, NFT],
            fname: Witness.Aux[FN],
            nfname: Witness.Aux[NFN],
            decomp: DecompPath.Aux[SP, P, FN],
            compute: ColumnOps.Compute[S, O, (Path.Aux[WP,SP], I2)]
        ): Aux[S, O, (Path.Aux[WP,SP], I2), P :: HNil, FieldType[NFN, NFT] :: HNil] = new Compute[S, O, (Path.Aux[WP,SP], I2)] {
            type AddTo = P :: HNil ; type NewF = FieldType[NFN, NFT] :: HNil
            def toSparkColumn: List[Column] = List(compute.toSparkColumn)
            def newNames: List[(String, String)] = List((nfname.value.name, fname.value.name))
        }

        implicit def operator_is_and[
            S <: HList, O1 <: AggOperator, O2 <: AggOperator, I1, I2, P1 <: HList, F1 <: HList, P2 <: HList, F2 <: HList, P <: HList, F <: HList
        ](
            implicit
            compute_first: AggOps.Compute.Aux[S, O1, I1, P1, F1],
            compute_second: AggOps.Compute.Aux[S, O2, I2, P2, F2],
            merge_p: Prepend.Aux[P1, P2, P],
            merge_f: Prepend.Aux[F1, F2, F]
        ): Aux[S, And[O1,O2], (I1,I2), P, F] = new Compute[S, And[O1,O2], (I1,I2)] {
            type AddTo = P ; type NewF = F
            def toSparkColumn: List[Column] = compute_first.toSparkColumn ++ compute_second.toSparkColumn
            def newNames: List[(String, String)] = compute_first.newNames ++ compute_second.newNames
        }

        protected trait AggOpOut[O <: AggOperator, In] { type Out ; type OPName <: Symbol }
        object AggOpOut {
            type Aux[O <: AggOperator, In, OPName0 <: Symbol, Out0] = AggOpOut[O, In] { type Out = Out0 ; type OPName = OPName0 }

            private type Numbers = Int :: Double :: Long :: Short :: Float :: HNil

            implicit def op_is_count[In]: Aux[Count, In, Witness.`'count`.T, Long] = new AggOpOut[Count, In] { type Out = Long ; type OPName = Witness.`'count`.T }
            implicit def op_is_max[In]: Aux[Max, In, Witness.`'max`.T, In] = new AggOpOut[Max, In] { type Out = In ; type OPName = Witness.`'max`.T }
            implicit def op_is_min[In]: Aux[Min, In, Witness.`'min`.T, In] = new AggOpOut[Min, In] { type Out = In ; type OPName = Witness.`'min`.T }
            implicit def op_is_avg[In](
                implicit
                inIsNumber: Selector[Numbers, In]
            ): Aux[Avg, In, Witness.`'avg`.T, Double] = new AggOpOut[Avg, In] { type Out = Double ; type OPName = Witness.`'avg`.T }
            implicit def op_is_median[In]: Aux[Median, In, Witness.`'median`.T, In] = new AggOpOut[Median, In] { type Out = In ; type OPName = Witness.`'median`.T }
            implicit def op_is_sum[In](
                implicit
                inIsNumber: Selector[Numbers, In]
            ): Aux[Sum, In, Witness.`'sum`.T, In] = new AggOpOut[Sum, In] { type Out = In ; type OPName = Witness.`'sum`.T }
            implicit def op_is_product[In](
                implicit
                inIsNumber: Selector[Numbers, In]
            ): Aux[Product, In, Witness.`'product`.T, In] = new AggOpOut[Product, In] { type Out = In ; type OPName = Witness.`'product`.T }
        }        
    }
}