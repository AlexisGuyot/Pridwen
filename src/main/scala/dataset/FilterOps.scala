package pridwen.dataset

import pridwen.types.opschema.SelectField

import shapeless.{HList, Witness, HNil, ::, Widen}

import org.apache.spark.sql.{Column}
import org.apache.spark.sql.functions.{col}

trait FilterOps[O, I]
object FilterOps {
    trait FOperator
    trait Equal extends FOperator
    trait Different extends FOperator
    trait MoreThan extends FOperator
    trait LessThan extends FOperator
    trait MoreOrEqual extends FOperator
    trait LessOrEqual extends FOperator
    trait IsNull extends FOperator
    trait IsNotNull extends FOperator
    //trait IsIn extends FOperator
    //trait Between extends FOperator
    //trait Like extends FOperator

    trait And[O1 <: FOperator, O2 <: FOperator] ; trait Or[O1 <: FOperator, O2 <: FOperator]

    trait Compute[S <: HList, O, I] { def toSparkColumn: Column }
    object Compute {
        implicit def second_input_is_value[
            S <: HList, WP1 <: HList, SP1 <: HList, FN1 <: Symbol, V, FT >: V, O <: FOperator
        ](
            implicit
            select_f1: SelectField.Aux[S, SP1, FN1, FT],
            get_path1: Path.Aux[WP1,SP1],
            singleton: Witness.Aux[V],
            compare: SparkOperator[O]
        ): Compute[S, O, (Path.Aux[WP1,SP1], Widen.Aux[V, FT])] = new Compute[S, O, (Path.Aux[WP1,SP1], Widen.Aux[V, FT])] {
            def toSparkColumn: Column = compare(col(get_path1.asString), singleton.value)
        }

        implicit def second_input_is_path[
            S <: HList, WP1 <: HList, SP1 <: HList, WP2 <: HList, SP2 <: HList, FN1 <: Symbol, FN2 <: Symbol, FT, O <: FOperator
        ](
            implicit
            select_f1: SelectField.Aux[S, SP1, FN1, FT],
            select_f2: SelectField.Aux[S, SP2, FN2, FT],
            get_path1: Path.Aux[WP1,SP1],
            get_path2: Path.Aux[WP2,SP2],
            compare: SparkOperator[O]
        ): Compute[S, O, (Path.Aux[WP1,SP1], Path.Aux[WP2,SP2])] = new Compute[S, O, (Path.Aux[WP1,SP1], Path.Aux[WP2,SP2])] {
            def toSparkColumn: Column = compare(col(get_path1.asString), col(get_path2.asString))
        }

        implicit def second_input_is_witness[
            S <: HList, WP1 <: HList, SP1 <: HList, FN1 <: Symbol, FN2 <: Symbol, V, FT >: V, O <: FOperator
        ](
            implicit
            select_f1: SelectField.Aux[S, SP1, FN1, FT],
            select_f2: SelectField.Aux[S, FN2 :: HNil, FN2, FT],
            get_path1: Path.Aux[WP1,SP1],
            get_path2: Path[Witness.Aux[FN2] :: HNil],
            compare: SparkOperator[O]
        ): Compute[S, O, (Path.Aux[WP1,SP1], Witness.Aux[FN2])] = new Compute[S, O, (Path.Aux[WP1,SP1], Witness.Aux[FN2])] {
            def toSparkColumn: Column = compare(col(get_path1.asString), col(get_path2.asString))
        }

        implicit def operator_is_and[
            S <: HList, O1 <: FOperator, O2 <: FOperator, I1, I2
        ](
            implicit
            compute_first: Compute[S, O1,I1],
            compute_second: Compute[S, O2,I2]
        ): Compute[S, And[O1,O2], (I1,I2)] = new Compute[S, And[O1,O2], (I1,I2)] {
            def toSparkColumn: Column = compute_first.toSparkColumn && compute_second.toSparkColumn
        }

        implicit def operator_is_or[
            S <: HList, O1 <: FOperator, O2 <: FOperator, I1, I2
        ](
            implicit
            compute_first: Compute[S, O1,I1],
            compute_second: Compute[S, O2,I2]
        ): Compute[S, Or[O1,O2], (I1,I2)] = new Compute[S, Or[O1,O2], (I1,I2)] {
            def toSparkColumn: Column = compute_first.toSparkColumn || compute_second.toSparkColumn
        }

        protected trait SparkOperator[O <: FOperator] { def apply(c1: Column, c2: Any): Column }
        object SparkOperator {
            implicit def op_is_equal: SparkOperator[Equal] 
                = new SparkOperator[Equal] { def apply(c1: Column, c2: Any) = c1 === c2 }

            implicit def op_is_different: SparkOperator[Different] 
                = new SparkOperator[Different] { def apply(c1: Column, c2: Any) = c1 =!= c2 }

            implicit def op_is_more: SparkOperator[MoreThan] 
                = new SparkOperator[MoreThan] { def apply(c1: Column, c2: Any) = c1 > c2 }

            implicit def op_is_less: SparkOperator[LessThan] 
                = new SparkOperator[LessThan] { def apply(c1: Column, c2: Any) = c1 < c2 }

            implicit def op_is_more_or_equal: SparkOperator[MoreOrEqual] 
                = new SparkOperator[MoreOrEqual] { def apply(c1: Column, c2: Any) = c1 >= c2 }

            implicit def op_is_less_or_equal: SparkOperator[LessOrEqual] 
                = new SparkOperator[LessOrEqual] { def apply(c1: Column, c2: Any) = c1 <= c2 }

            implicit def op_is_null: SparkOperator[IsNull] 
                = new SparkOperator[IsNull] { def apply(c1: Column, c2: Any) = c1.isNull }

            implicit def op_is_not_null: SparkOperator[IsNotNull] 
                = new SparkOperator[IsNotNull] { def apply(c1: Column, c2: Any) = c1.isNotNull }
        }
    }
}