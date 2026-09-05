# Linked HTML images

`Test.as` exercises a real SymbolClass-linked MovieClip and the original
`htmlMC.InitStage` initialization sequence. `render/Test.as` checks actual pixels,
including visibility changes inside a cached ancestor. The linked sprite has two
frames so the trace test also checks orphan timeline advancement.

The position assertions come from a unit-scale Flash Player probe: default image
spacing is 8 on each side, left images start at the field's 2-pixel gutter plus
hspace, right images use the full field width, and consecutive images stack.
Image spaces have no character bounds and images do not increase textHeight.
The trace test uses the test runner's default font only for font-independent
relations (exclusion offsets, wrapping, and line-height-relative placement).

To rebuild both static SWFs, run here with a locally built playerglobal import:

```sh
python3 compile.py /absolute/path/to/playerglobal_import.abc
python3 compile.py /absolute/path/to/playerglobal_import.abc render/Test.as
```

The compiler script reuses ASC and attaches a real DefineSprite/SymbolClass pair;
it does not fake `getImageReference` or patch game sources. Run both tests with:

```sh
cargo test -p tests --features imgtests --test tests -- avm2/edittext_html_linked_image
```

This regression intentionally does not require Flash's negative glyph-width
quirk around a mid-text image. Remote images and baseline alignment are outside
this local-symbol implementation.
