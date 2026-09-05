"""Compile with ASC, then attach a real SymbolClass linkage to an empty sprite.

Run from this directory with playerglobal_import.abc as the first argument.
An optional second argument selects render/Test.as instead of Test.as.
The sprite constructor draws its content, so no authoring tool is required.
"""
from pathlib import Path
import struct
import subprocess
import sys
import zlib

root = next(path for path in Path.cwd().parents if (path / "tools/asc/asc.jar").exists())
source = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("Test.as")
subprocess.run([
    "java", "-jar", str(root / "tools/asc/asc.jar"), "-AS3", "-import", sys.argv[1],
    "-in", "ProbeImage.as", "-swf", "Test,400,240,30", str(source),
], check=True)
data = source.with_suffix(".swf").read_bytes()
body = zlib.decompress(data[8:]) if data[:3] == b"CWS" else data[8:]
position = (5 + 4 * (body[0] >> 3) + 7) // 8 + 4


def tag(code, payload=b""):
    assert len(payload) < 63
    return struct.pack("<H", (code << 6) | len(payload)) + payload


while position < len(body):
    header = struct.unpack_from("<H", body, position)[0]
    code, length = header >> 6, header & 63
    header_length = 2
    if length == 63:
        length = struct.unpack_from("<I", body, position + 2)[0]
        header_length = 6
    if code == 1:
        sprite = tag(39, struct.pack("<HH", 1, 2) + tag(1) + tag(1) + tag(0))
        linkage = tag(76, struct.pack("<HH", 1, 1) + b"ProbeImage\0")
        body = body[:position] + sprite + linkage + body[position:]
        break
    position += header_length + length
else:
    raise AssertionError("ShowFrame missing")
source.with_name("test.swf").write_bytes(b"FWS" + data[3:4] + struct.pack("<I", 8 + len(body)) + body)
source.with_suffix(".swf").unlink()
source.with_suffix(".abc").unlink()
