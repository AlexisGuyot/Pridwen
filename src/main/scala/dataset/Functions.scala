package pridwen.dataset

import shapeless.{Witness, HNil, ::, HList, Widen}
import shapeless.ops.hlist.Prepend

object functions {
    def col[FN <: Symbol](a: Witness.Aux[FN]): Witness.Aux[FN] = a
    //def v[V](c: Witness.Aux[V]): Witness.Aux[V] = c
    def v[V, T >: V](c: Witness.Aux[V])(implicit w: Widen.Aux[V, T]): Widen.Aux[V, T] = w

    implicit class ExtendWitness[FN1 <: Symbol](f1: Witness.Aux[FN1]) {
        def ->[FN2 <: Symbol](f2: Witness.Aux[FN2])(implicit path: Path[Witness.Aux[FN1] :: Witness.Aux[FN2] :: HNil]): Path.Aux[Witness.Aux[FN1] :: Witness.Aux[FN2] :: HNil, path.T] = path
        def &&[WP <: HList, SP <: HList](p: Path.Aux[WP, SP])(implicit paths: MultiplePaths[Witness.Aux[FN1] :: Path.Aux[WP, SP] :: HNil]): MultiplePaths.Aux[Witness.Aux[FN1] :: Path.Aux[WP, SP] :: HNil, paths.T] = paths
        def &&[WP <: HList, NN <: Symbol, SP <: HList](p: Path.As.Aux[WP, NN, SP])(implicit paths: MultiplePaths[Witness.Aux[FN1] :: Path.As.Aux[WP, NN, SP] :: HNil]): MultiplePaths.Aux[Witness.Aux[FN1] :: Path.As.Aux[WP, NN, SP] :: HNil, paths.T] = paths
        def as[FN <: Symbol](alias: Witness.Aux[FN])(implicit path: Path.As[Witness.Aux[FN1] :: HNil, FN]): Path.As.Aux[Witness.Aux[FN1] :: HNil, FN, path.T] = path

        def ===[T](f2: T)(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new FilterOps[FilterOps.Equal, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], T)] {}
        def =!=[T](f2: T)(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new FilterOps[FilterOps.Different, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], T)] {}
        def >[T](f2: T)(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new FilterOps[FilterOps.MoreThan, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], T)] {}
        def <[T](f2: T)(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new FilterOps[FilterOps.LessThan, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], T)] {}
        def >=[T](f2: T)(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new FilterOps[FilterOps.MoreOrEqual, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], T)] {}
        def <=[T](f2: T)(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new FilterOps[FilterOps.LessOrEqual, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], T)] {}
        def isNull(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new FilterOps[FilterOps.IsNull, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], Path.Aux[Witness.Aux[FN1] :: HNil, p1.T])] {}
        def isNotNull(implicit p1: Path[Witness.Aux[FN1] :: HNil]) = new FilterOps[FilterOps.IsNotNull, (Path.Aux[Witness.Aux[FN1] :: HNil, p1.T], Path.Aux[Witness.Aux[FN1] :: HNil, p1.T])] {}
    }

    implicit class ExtendPath[WP1 <: HList, SP1 <: HList](p: Path.Aux[WP1, SP1]) {
        def ->[FN <: Symbol, P <: HList](f: Witness.Aux[FN])(implicit prepend: Prepend.Aux[WP1, Witness.Aux[FN] :: HNil, P], path: Path[P]): Path.Aux[P, path.T] = path
        def &&[WP2 <: HList, SP2 <: HList](p2: Path.Aux[WP2, SP2])(implicit paths: MultiplePaths[Path.Aux[WP1, SP1] :: Path.Aux[WP2, SP2] :: HNil]): MultiplePaths.Aux[Path.Aux[WP1, SP1] :: Path.Aux[WP2, SP2] :: HNil, paths.T] = paths
        def &&[WP2 <: HList, NN <: Symbol, SP2 <: HList](p2: Path.As.Aux[WP2, NN, SP2])(implicit paths: MultiplePaths[Path.Aux[WP1, SP1] :: Path.As.Aux[WP2, NN, SP2] :: HNil]): MultiplePaths.Aux[Path.Aux[WP1, SP1] :: Path.As.Aux[WP2, NN, SP2] :: HNil, paths.T] = paths
        def as[FN <: Symbol](alias: Witness.Aux[FN])(implicit path: Path.As[WP1, FN]): Path.As.Aux[WP1, FN, path.T] = path

        def ===[T](f2: T) = new FilterOps[FilterOps.Equal, (Path.Aux[WP1, SP1], T)] {}
        def =!=[T](f2: T) = new FilterOps[FilterOps.Different, (Path.Aux[WP1, SP1], T)] {}
        def >[T](f2: T) = new FilterOps[FilterOps.MoreThan, (Path.Aux[WP1, SP1], T)] {}
        def <[T](f2: T) = new FilterOps[FilterOps.LessThan, (Path.Aux[WP1, SP1], T)] {}
        def >=[T](f2: T) = new FilterOps[FilterOps.MoreOrEqual, (Path.Aux[WP1, SP1], T)] {}
        def <=[T](f2: T) = new FilterOps[FilterOps.LessOrEqual, (Path.Aux[WP1, SP1], T)] {}
        def isNull = new FilterOps[FilterOps.IsNull, (Path.Aux[WP1, SP1], Path.Aux[WP1, SP1])] {}
        def isNotNull = new FilterOps[FilterOps.IsNotNull, (Path.Aux[WP1, SP1], Path.Aux[WP1, SP1])] {}
    }

    implicit class ExtendMultiplePaths[MP <: HList, MPS <: HList](mp: MultiplePaths.Aux[MP, MPS]) {
        def &&[WP <: HList, SP <: HList, NMP <: HList](p: Path.Aux[WP, SP])(implicit prepend: Prepend.Aux[MP, Path.Aux[WP, SP] :: HNil, NMP], paths: MultiplePaths[NMP]): MultiplePaths.Aux[NMP, paths.T] = paths
        def &&[WP <: HList, NN <: Symbol, SP <: HList, NMP <: HList](p: Path.As.Aux[WP, NN, SP])(implicit prepend: Prepend.Aux[MP, Path.As.Aux[WP, NN, SP] :: HNil, NMP], paths: MultiplePaths[NMP]): MultiplePaths.Aux[NMP, paths.T] = paths
    }

    implicit class ExtendFilterOps[O1 <: FilterOps.FOperator,I1](f1: FilterOps[O1,I1]) {
        def &&[O2 <: FilterOps.FOperator, I2](f2: FilterOps[O2, I2]) = new FilterOps[FilterOps.And[O1,O2], (I1,I2)] {}
        def ||[O2 <: FilterOps.FOperator, I2](f2: FilterOps[O2, I2]) = new FilterOps[FilterOps.Or[O1,O2], (I1,I2)] {}
    }
}