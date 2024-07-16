package pridwen.types.support

import shapeless.labelled.{FieldType}

object functions {
    def fieldToValue[FN, FT](f: FieldType[FN, FT]): FT = f
}