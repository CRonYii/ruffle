"""Generate tiny synthetic selection fixtures, not copies of system fonts.

Requires fontTools only to regenerate; Rust tests embed the generated TTFs.
Family names deliberately match the Windows candidates under test.
"""
from pathlib import Path
from fontTools.fontBuilder import FontBuilder
from fontTools.pens.ttGlyphPen import TTGlyphPen


def build(filename, family, bold, cmap):
    order = [".notdef", *cmap.values()]
    fb = FontBuilder(1000, isTTF=True)
    fb.setupGlyphOrder(order)
    fb.setupCharacterMap(cmap)
    glyphs = {}
    for name in order:
        pen = TTGlyphPen(None)
        if name != ".notdef":
            pen.moveTo((50, 0))
            pen.lineTo((550, 0))
            pen.lineTo((300, 700))
            pen.closePath()
        glyphs[name] = pen.glyph()
    fb.setupGlyf(glyphs)
    fb.setupHorizontalMetrics({name: (600, 0) for name in order})
    fb.setupHorizontalHeader(ascent=800, descent=-200)
    style = "Bold" if bold else "Regular"
    fb.setupNameTable({"familyName": family, "styleName": style,
                       "uniqueFontIdentifier": f"Ruffle synthetic {family} {style}",
                       "fullName": f"{family} {style}",
                       "psName": f"RuffleSynthetic{filename.removesuffix('.ttf')}"})
    fb.setupOS2(sTypoAscender=800, sTypoDescender=-200, usWinAscent=800,
                usWinDescent=200, usWeightClass=700 if bold else 400,
                fsSelection=0x20 if bold else 0x40)
    fb.setupPost()
    fb.setupMaxp()
    fb.font["head"].macStyle = 1 if bold else 0
    fb.font["head"].created = fb.font["head"].modified = 3400000000
    fb.font.recalcTimestamp = False
    fb.save(Path(__file__).with_name(filename))


if __name__ == "__main__":
    build("latin.ttf", "Times New Roman", False, {0x41: "A"})
    build("latin-bold.ttf", "Times New Roman", True, {0x41: "A"})
    build("cjk.ttf", "SimSun", False, {0x4E2D: "uni4E2D", 0x6587: "uni6587"})
