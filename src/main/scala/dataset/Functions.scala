package pridwen.dataset

import shapeless.{Witness, HNil, ::, HList}
import shapeless.ops.hlist.Prepend

object functions {
    def col[FN <: Symbol](a: Witness.Aux[FN]): Witness.Aux[FN] = a

    implicit class ExtendWitness[FN1 <: Symbol](f1: Witness.Aux[FN1]) {
        def ->[FN2 <: Symbol](f2: Witness.Aux[FN2])(implicit path: Path[Witness.Aux[FN1] :: Witness.Aux[FN2] :: HNil]): Path.Aux[Witness.Aux[FN1] :: Witness.Aux[FN2] :: HNil, path.T] = path
        def |[FN2 <: Symbol](f2: Witness.Aux[FN2])(implicit paths: MultiplePaths[Witness.Aux[FN1] :: Witness.Aux[FN2] :: HNil]): MultiplePaths.Aux[Witness.Aux[FN1] :: Witness.Aux[FN2] :: HNil, paths.T] = paths
        def |[WP <: HList, SP <: HList](p: Path.Aux[WP, SP])(implicit paths: MultiplePaths[Witness.Aux[FN1] :: Path.Aux[WP, SP] :: HNil]): MultiplePaths.Aux[Witness.Aux[FN1] :: Path.Aux[WP, SP] :: HNil, paths.T] = paths
    }

    implicit class ExtendPath[SPW <: HList, SPS <: HList](subpath: Path.Aux[SPW, SPS]) {
        def ->[FN <: Symbol, P <: HList](f: Witness.Aux[FN])(implicit prepend: Prepend.Aux[SPW, Witness.Aux[FN] :: HNil, P], path: Path[P]): Path.Aux[P, path.T] = path
        def |[FN <: Symbol](f2: Witness.Aux[FN])(implicit paths: MultiplePaths[Path[SPW] :: Witness.Aux[FN] :: HNil]): MultiplePaths.Aux[Path[SPW] :: Witness.Aux[FN] :: HNil, paths.T] = paths
        def |[WP <: HList, SP <: HList](p: Path.Aux[WP, SP])(implicit paths: MultiplePaths[Path.Aux[SPW, SPS] :: Path.Aux[WP, SP] :: HNil]): MultiplePaths.Aux[Path.Aux[SPW, SPS] :: Path.Aux[WP, SP] :: HNil, paths.T] = paths
    }

    implicit class ExtendMultiplePaths[MP <: HList, MPS <: HList](mp: MultiplePaths.Aux[MP, MPS]) {
        def |[FN <: Symbol, NMP <: HList](f2: Witness.Aux[FN])(implicit prepend: Prepend.Aux[MP, Witness.Aux[FN] :: HNil, NMP], paths: MultiplePaths[NMP]): MultiplePaths.Aux[NMP, paths.T] = paths
        def |[WP <: HList, SP <: HList, NMP <: HList](p: Path.Aux[WP, SP])(implicit prepend: Prepend.Aux[MP, Path.Aux[WP, SP] :: HNil, NMP], paths: MultiplePaths[NMP]): MultiplePaths.Aux[NMP, paths.T] = paths
    }
}