package pridwen.dataset

import pridwen.types.opschema.SelectField

import shapeless.{HList, Witness, HNil, ::, Widen}

import org.apache.spark.sql.{Column}
import org.apache.spark.sql.functions.{col}

object ColumnOps {
    trait COperator

    trait FOperator extends COperator
    trait AOperator extends COperator
    trait OOperator extends COperator

    trait Equal extends FOperator with AOperator
    trait Different extends FOperator with AOperator
    trait MoreThan extends FOperator with AOperator
    trait LessThan extends FOperator with AOperator
    trait MoreOrEqual extends FOperator with AOperator
    trait LessOrEqual extends FOperator with AOperator
    trait IsNull extends FOperator with AOperator
    trait IsNotNull extends FOperator with AOperator
    //trait IsIn extends FOperator with AOperator
    //trait Between extends FOperator with AOperator
    //trait Like extends FOperator with AOperator

    trait Modulo extends AOperator
    trait Multiply extends AOperator
    trait Add extends AOperator
    trait Substract extends AOperator
    trait Divide extends AOperator

    trait Asc extends OOperator
    trait Desc extends OOperator

    trait SparkOperator[O <: COperator] { def apply(c1: Column, c2: Any): Column }
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

        implicit def op_is_modulo: SparkOperator[Modulo] 
            = new SparkOperator[Modulo] { def apply(c1: Column, c2: Any) = c1 % c2 }

        implicit def op_is_multiply: SparkOperator[Multiply] 
            = new SparkOperator[Multiply] { def apply(c1: Column, c2: Any) = c1 * c2 }

        implicit def op_is_add: SparkOperator[Add] 
            = new SparkOperator[Add] { def apply(c1: Column, c2: Any) = c1 + c2 }

        implicit def op_is_substract: SparkOperator[Substract] 
            = new SparkOperator[Substract] { def apply(c1: Column, c2: Any) = c1 - c2 }

        implicit def op_is_divide: SparkOperator[Divide] 
            = new SparkOperator[Divide] { def apply(c1: Column, c2: Any) = c1 / c2 }

        implicit def op_is_asc: SparkOperator[Asc] 
            = new SparkOperator[Asc] { def apply(c1: Column, c2: Any) = c1.asc }

        implicit def op_is_desc: SparkOperator[Desc] 
            = new SparkOperator[Desc] { def apply(c1: Column, c2: Any) = c1.desc }
    }

    trait Compute[S <: HList, O <: COperator, I] { def toSparkColumn: Column }
    object Compute {
        implicit def second_input_is_value[
            S <: HList, WP1 <: HList, SP1 <: HList, FN1 <: Symbol, V, FT >: V, O <: COperator
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
            S <: HList, WP1 <: HList, SP1 <: HList, WP2 <: HList, SP2 <: HList, FN1 <: Symbol, FN2 <: Symbol, FT, O <: COperator
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
            S <: HList, WP1 <: HList, SP1 <: HList, FN1 <: Symbol, FN2 <: Symbol, V, FT >: V, O <: COperator
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
    }
}