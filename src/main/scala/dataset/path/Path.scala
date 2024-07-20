package pridwen.dataset

import shapeless.{HList, HNil, ::, Witness}

import org.apache.spark.sql.{Column}
import org.apache.spark.sql.functions.{col}

import scala.annotation.implicitNotFound

@implicitNotFound("[Pridwen / Path] ${L} is not a path of symbols.") 
trait Path[L <: HList] { type T <: HList ; def asString: String }
object Path {
    type Aux[L <: HList, T0 <: HList] = Path[L] { type T = T0 }
    def apply[L <: HList](path: L)(implicit get_fnames: Path[L]): Path.Aux[L, get_fnames.T] = get_fnames

    implicit def end_of_path[FN <: Symbol](
        implicit
        field: Witness.Aux[FN]
    ): Path.Aux[Witness.Aux[FN] :: HNil, FN :: HNil] 
        = new Path[Witness.Aux[FN] :: HNil] { type T = FN :: HNil ; def asString = field.value.name }

    implicit def default[FN <: Symbol, PT <: HList, T0 <: HList](
        implicit
        field: Witness.Aux[FN],
        validTail: Path.Aux[PT, T0]
    ): Path.Aux[Witness.Aux[FN] :: PT, FN :: T0] = new Path[Witness.Aux[FN] :: PT] { 
        type T = FN :: T0 
        def asString = s"${field.value.name}.${validTail.asString}"
    }

    trait As[L <: HList, NN <: Symbol] { type T <: HList ; def asString: String }
    object As {
        type Aux[L <: HList, NN <: Symbol, T0 <: HList] = As[L, NN] { type T = T0 }
        implicit def path_with_alias[L <: HList, NN <: Symbol, T0 <: HList](
            implicit
            p: Path.Aux[L, T0]
        ): Aux[L, NN, T0] = new As[L,NN] { type T = T0 ; def asString = p.asString }
    }
}

trait MultiplePaths[L <: HList] { type T <: HList ; def toColumns: List[Column] }
object MultiplePaths {
    type Aux[L <: HList, T0 <: HList] = MultiplePaths[L] { type T = T0 }
    def apply[L <: HList](paths: L)(implicit get_paths: MultiplePaths[L]): MultiplePaths.Aux[L, get_paths.T] = get_paths

    implicit def one_path_left[P <: HList, T0 <: HList](
        implicit
        get_fnames: Path.Aux[P, T0]
    ): MultiplePaths.Aux[Path.Aux[P, T0] :: HNil, T0 :: HNil] 
        = new MultiplePaths[Path.Aux[P, T0] :: HNil] { type T = T0 :: HNil ; def toColumns = List(col(get_fnames.asString)) }

    implicit def one_path_with_alias_left[P <: HList, NN <: Symbol, T0 <: HList](
        implicit
        get_fnames: Path.As.Aux[P, NN, T0],
        new_name: Witness.Aux[NN]
    ): MultiplePaths.Aux[Path.As.Aux[P, NN, T0] :: HNil, (T0, NN) :: HNil] 
        = new MultiplePaths[Path.As.Aux[P, NN, T0] :: HNil] { type T = (T0, NN) :: HNil ; def toColumns = List(col(get_fnames.asString).as(new_name.value.name)) }

    implicit def one_witness_left[FN <: Symbol](
        implicit
        field: Witness.Aux[FN]
    ): MultiplePaths.Aux[Witness.Aux[FN] :: HNil, (FN :: HNil) :: HNil] 
        = new MultiplePaths[Witness.Aux[FN] :: HNil] { type T = (FN :: HNil) :: HNil ; def toColumns = List(col(field.value.name)) }

    implicit def default_path[P <: HList, OP <: HList, T0 <: HList, T1 <: HList](
        implicit
        get_fnames: Path.Aux[P, T0],
        get_opaths: MultiplePaths.Aux[OP, T1]
    ): MultiplePaths.Aux[Path.Aux[P, T0] :: OP, T0 :: T1] = new MultiplePaths[Path.Aux[P, T0] :: OP] { 
        type T = T0 :: T1 
        def toColumns = col(get_fnames.asString) :: get_opaths.toColumns
    }

    implicit def default_path_with_alias[P <: HList, OP <: HList, NN <: Symbol, T0 <: HList, T1 <: HList](
        implicit
        get_fnames: Path.As.Aux[P, NN, T0],
        get_opaths: MultiplePaths.Aux[OP, T1],
        new_name: Witness.Aux[NN]
    ): MultiplePaths.Aux[Path.As.Aux[P, NN, T0] :: OP, (T0, NN) :: T1] = new MultiplePaths[Path.As.Aux[P, NN, T0] :: OP] { 
        type T = (T0, NN) :: T1 
        def toColumns = col(get_fnames.asString).as(new_name.value.name) :: get_opaths.toColumns
    }

    implicit def default_witness[FN <: Symbol, OP <: HList, T1 <: HList](
        implicit
        field: Witness.Aux[FN],
        get_opaths: MultiplePaths.Aux[OP, T1]
    ): MultiplePaths.Aux[Witness.Aux[FN] :: OP, (FN :: HNil) :: T1] = new MultiplePaths[Witness.Aux[FN] :: OP] { 
        type T = (FN :: HNil) :: T1
        def toColumns = col(field.value.name) :: get_opaths.toColumns
    }
}