package pridwen.dataset

import pridwen.types.models.{Model}
import pridwen.types.support.{DeepLabelledGeneric => LabelledGeneric}
import ColumnOps._

import org.apache.spark.sql.Dataset

import shapeless.{Witness, HNil, ::, HList, Widen}
import shapeless.ops.hlist.Prepend

object implicits {
    implicit class DatasetExtension[S, HS <: HList](ds: Dataset[S])(implicit toHList: LabelledGeneric.Aux[S, HS]) {
        def asModel[M <: Model](implicit isValid: Model.As[HS, M]) = new Data[M, HS] { type DST = S ; val data = ds }
    }

    implicit def witnessToPath[FN <: Symbol, PS <: HList](f: Witness.Aux[FN])(implicit p: Path[Witness.Aux[FN] :: HNil]): Path.Aux[Witness.Aux[FN] :: HNil, p.T] = p
    implicit def witnessToMultiple[FN <: Symbol](f: Witness.Aux[FN])(implicit mp: MultiplePaths[Witness.Aux[FN] :: HNil]): MultiplePaths.Aux[Witness.Aux[FN] :: HNil, mp.T] = mp
    implicit def pathToMultiple[PW <: HList, PS <: HList, MPS <: HList](p: Path.Aux[PW, PS])(implicit mp: MultiplePaths[Path.Aux[PW, PS] :: HNil]): MultiplePaths.Aux[Path.Aux[PW, PS] :: HNil, mp.T] = mp

    implicit class ExtendWitness[FN1 <: Symbol](f1: Witness.Aux[FN1]) {
        def ->[FN2 <: Symbol](f2: Witness.Aux[FN2])(implicit path: Path[Witness.Aux[FN1] :: Witness.Aux[FN2] :: HNil]): Path.Aux[Witness.Aux[FN1] :: Witness.Aux[FN2] :: HNil, path.T] = path
        def &&[WP <: HList, SP <: HList](p: Path.Aux[WP, SP])(implicit paths: MultiplePaths[Witness.Aux[FN1] :: Path.Aux[WP, SP] :: HNil]): MultiplePaths.Aux[Witness.Aux[FN1] :: Path.Aux[WP, SP] :: HNil, paths.T] = paths
        def &&[WP <: HList, NN <: Symbol, SP <: HList](p: Path.As.Aux[WP, NN, SP])(implicit paths: MultiplePaths[Witness.Aux[FN1] :: Path.As.Aux[WP, NN, SP] :: HNil]): MultiplePaths.Aux[Witness.Aux[FN1] :: Path.As.Aux[WP, NN, SP] :: HNil, paths.T] = paths
        def as[FN <: Symbol](alias: Witness.Aux[FN])(implicit path: Path.As[Witness.Aux[FN1] :: HNil, FN]): Path.As.Aux[Witness.Aux[FN1] :: HNil, FN, path.T] = path

        def ===[T](f2: T)(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new FilterOps[Equal, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], T)] {}
        def =!=[T](f2: T)(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new FilterOps[Different, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], T)] {}
        def >[T](f2: T)(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new FilterOps[MoreThan, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], T)] {}
        def <[T](f2: T)(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new FilterOps[LessThan, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], T)] {}
        def >=[T](f2: T)(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new FilterOps[MoreOrEqual, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], T)] {}
        def <=[T](f2: T)(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new FilterOps[LessOrEqual, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], T)] {}
        def isNull(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new FilterOps[IsNull, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], Path.Aux[Witness.Aux[FN1] :: HNil, p1.T])] {}
        def isNotNull(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new FilterOps[IsNotNull, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], Path.Aux[Witness.Aux[FN1] :: HNil, p1.T])] {}
    
        def %[T](f2: T)(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new AddOps[Modulo, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], T)] {}
        def *[T](f2: T)(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new AddOps[Multiply, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], T)] {}
        def +[T](f2: T)(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new AddOps[Add, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], T)] {}
        def -[T](f2: T)(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new AddOps[Substract, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], T)] {}
        def /[T](f2: T)(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new AddOps[Divide, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], T)] {}
    
        def asc(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new OrderOps[Asc, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], Path.Aux[Witness.Aux[FN1] :: HNil, p1.T])] {}
        def desc(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new OrderOps[Desc, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], Path.Aux[Witness.Aux[FN1] :: HNil, p1.T])] {}

        def count(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new AggOps[Count, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], Path.Aux[Witness.Aux[FN1] :: HNil, p1.T])] {}
        def max(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new AggOps[Max, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], Path.Aux[Witness.Aux[FN1] :: HNil, p1.T])] {}
        def min(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new AggOps[Min, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], Path.Aux[Witness.Aux[FN1] :: HNil, p1.T])] {}
        def avg(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new AggOps[Avg, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], Path.Aux[Witness.Aux[FN1] :: HNil, p1.T])] {}
        def median(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new AggOps[Median, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], Path.Aux[Witness.Aux[FN1] :: HNil, p1.T])] {}
        def sum(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new AggOps[Sum, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], Path.Aux[Witness.Aux[FN1] :: HNil, p1.T])] {}
        def product(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new AggOps[Product, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], Path.Aux[Witness.Aux[FN1] :: HNil, p1.T])] {}
    }

    implicit class ExtendPath[WP1 <: HList, SP1 <: HList](p: Path.Aux[WP1, SP1]) {
        def ->[FN <: Symbol, P <: HList](f: Witness.Aux[FN])(implicit prepend: Prepend.Aux[WP1, Witness.Aux[FN] :: HNil, P], path: Path[P]): Path.Aux[P, path.T] = path
        def &&[WP2 <: HList, SP2 <: HList](p2: Path.Aux[WP2, SP2])(implicit paths: MultiplePaths[Path.Aux[WP1, SP1] :: Path.Aux[WP2, SP2] :: HNil]): MultiplePaths.Aux[Path.Aux[WP1, SP1] :: Path.Aux[WP2, SP2] :: HNil, paths.T] = paths
        def &&[WP2 <: HList, NN <: Symbol, SP2 <: HList](p2: Path.As.Aux[WP2, NN, SP2])(implicit paths: MultiplePaths[Path.Aux[WP1, SP1] :: Path.As.Aux[WP2, NN, SP2] :: HNil]): MultiplePaths.Aux[Path.Aux[WP1, SP1] :: Path.As.Aux[WP2, NN, SP2] :: HNil, paths.T] = paths
        def as[FN <: Symbol](alias: Witness.Aux[FN])(implicit path: Path.As[WP1, FN]): Path.As.Aux[WP1, FN, path.T] = path

        def ===[T](f2: T) = new FilterOps[Equal, (Path.Aux[WP1, SP1], T)] {}
        def =!=[T](f2: T) = new FilterOps[Different, (Path.Aux[WP1, SP1], T)] {}
        def >[T](f2: T) = new FilterOps[MoreThan, (Path.Aux[WP1, SP1], T)] {}
        def <[T](f2: T) = new FilterOps[LessThan, (Path.Aux[WP1, SP1], T)] {}
        def >=[T](f2: T) = new FilterOps[MoreOrEqual, (Path.Aux[WP1, SP1], T)] {}
        def <=[T](f2: T) = new FilterOps[LessOrEqual, (Path.Aux[WP1, SP1], T)] {}
        def isNull = new FilterOps[IsNull, (Path.Aux[WP1, SP1], Path.Aux[WP1, SP1])] {}
        def isNotNull = new FilterOps[IsNotNull, (Path.Aux[WP1, SP1], Path.Aux[WP1, SP1])] {}

        def %[T](f2: T) = new AddOps[Modulo, (Path.Aux[WP1, SP1], T)] {}
        def *[T](f2: T) = new AddOps[Multiply, (Path.Aux[WP1, SP1], T)] {}
        def +[T](f2: T) = new AddOps[Add, (Path.Aux[WP1, SP1], T)] {}
        def -[T](f2: T) = new AddOps[Substract, (Path.Aux[WP1, SP1], T)] {}
        def /[T](f2: T) = new AddOps[Divide, (Path.Aux[WP1, SP1], T)] {}

        def asc = new OrderOps[Asc, (Path.Aux[WP1, SP1], Path.Aux[WP1, SP1])] {}
        def desc = new OrderOps[Desc, (Path.Aux[WP1, SP1], Path.Aux[WP1, SP1])] {}

        def count = new AggOps[Count, (Path.Aux[WP1, SP1], Path.Aux[WP1, SP1])] {}
        def max = new AggOps[Max, (Path.Aux[WP1, SP1], Path.Aux[WP1, SP1])] {}
        def min = new AggOps[Min, (Path.Aux[WP1, SP1], Path.Aux[WP1, SP1])] {}
        def avg = new AggOps[Avg, (Path.Aux[WP1, SP1], Path.Aux[WP1, SP1])] {}
        def median = new AggOps[Median, (Path.Aux[WP1, SP1], Path.Aux[WP1, SP1])] {}
        def sum = new AggOps[Sum, (Path.Aux[WP1, SP1], Path.Aux[WP1, SP1])] {}
        def product = new AggOps[Product, (Path.Aux[WP1, SP1], Path.Aux[WP1, SP1])] {}
    }

    implicit class ExtendMultiplePaths[MP <: HList, MPS <: HList](mp: MultiplePaths.Aux[MP, MPS]) {
        def &&[WP <: HList, SP <: HList, NMP <: HList](p: Path.Aux[WP, SP])(implicit prepend: Prepend.Aux[MP, Path.Aux[WP, SP] :: HNil, NMP], paths: MultiplePaths[NMP]): MultiplePaths.Aux[NMP, paths.T] = paths
        def &&[WP <: HList, NN <: Symbol, SP <: HList, NMP <: HList](p: Path.As.Aux[WP, NN, SP])(implicit prepend: Prepend.Aux[MP, Path.As.Aux[WP, NN, SP] :: HNil, NMP], paths: MultiplePaths[NMP]): MultiplePaths.Aux[NMP, paths.T] = paths
    }

    implicit class ExtendFilterOps[O1 <: FOperator,I1](f1: FilterOps[O1,I1]) {
        def &&[O2 <: FOperator, I2](f2: FilterOps[O2, I2]) = new FilterOps[FilterOps.And[O1,O2], (I1,I2)] {}
        def ||[O2 <: FOperator, I2](f2: FilterOps[O2, I2]) = new FilterOps[FilterOps.Or[O1,O2], (I1,I2)] {}
    }

    implicit class ExtendOrderOps[O1 <: OOperator,I1](f1: OrderOps[O1,I1]) {
        def &&[O2 <: OOperator, I2](f2: OrderOps[O2, I2]) = new OrderOps[OrderOps.And[O1,O2], (I1,I2)] {}
    }

    implicit class ExtendAggOps[O1 <: AggOperator,I1](f1: AggOps[O1,I1]) {
        def &&[O2 <: AggOperator, I2](f2: AggOps[O2, I2]) = new AggOps[AggOps.And[O1,O2], (I1,I2)] {}
    }
}

object functions {
    def col[FN <: Symbol](a: Witness.Aux[FN]): Witness.Aux[FN] = a
    //def v[V](c: Witness.Aux[V]): Witness.Aux[V] = c
    def v[V, T >: V](c: Witness.Aux[V])(implicit w: Widen.Aux[V, T]): Widen.Aux[V, T] = w
}