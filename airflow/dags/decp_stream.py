"""
DECP - stream the consolidated JSON into JSON Lines
The consolidated files are one JSON document each, {"marches": {"marche": [...],
"contrat-concession": [...]}}, 700 MB a year. Spark reads such a document as a
single record and a json.load needs several GB, so the records are cut out of
the stream one by one and written as JSON Lines, unchanged.
"""
import json

_DECODER = json.JSONDecoder()
_WHITESPACE = " \t\r\n"


class _Buffer:
    def __init__(self, chunks):
        self._chunks = iter(chunks)
        self.text = ""
        self.pos = 0

    def fill(self):
        chunk = next(self._chunks, None)
        if chunk is None:
            return False
        self.text = self.text[self.pos:] + chunk
        self.pos = 0
        return True

    def skip(self, chars):
        while True:
            while self.pos < len(self.text) and self.text[self.pos] in chars:
                self.pos += 1
            if self.pos < len(self.text) or not self.fill():
                return

    def peek(self):
        self.skip(_WHITESPACE)
        return self.text[self.pos] if self.pos < len(self.text) else ""

    def expect(self, char):
        if self.peek() != char:
            raise ValueError(f"expected {char!r} at offset {self.pos}, got {self.peek()!r}")
        self.pos += 1

    def value(self):
        self.peek()
        while True:
            try:
                obj, end = _DECODER.raw_decode(self.text, self.pos)
            except json.JSONDecodeError:
                if not self.fill():
                    raise
                continue
            # A number at the end of the buffer may be cut in two.
            if end == len(self.text) and self.fill():
                continue
            self.pos = end
            return obj


def iter_records(chunks):
    """Yield (kind, record) for each record of each array under "marches"."""
    buf = _Buffer(chunks)
    buf.expect("{")
    if buf.value() != "marches":
        raise ValueError("the document does not start with a 'marches' object")
    buf.expect(":")
    buf.expect("{")
    while buf.peek() != "}":
        kind = buf.value()
        buf.expect(":")
        buf.expect("[")
        while buf.peek() != "]":
            yield kind, buf.value()
            if buf.peek() == ",":
                buf.pos += 1
        buf.expect("]")
        if buf.peek() == ",":
            buf.pos += 1


def decode_chunks(raw, size=8 * 1024 * 1024):
    """UTF-8 text chunks from a binary stream, never splitting a character."""
    import codecs

    decoder = codecs.getincrementaldecoder("utf-8")()
    while True:
        data = raw.read(size)
        if not data:
            tail = decoder.decode(b"", final=True)
            if tail:
                yield tail
            return
        yield decoder.decode(data)


def jsonl_parts(records, rows_per_part=50_000):
    """Group (kind, record) into gzipped JSON Lines parts: yield (kind, index, bytes)."""
    import gzip
    import io

    open_parts = {}

    def flush(kind):
        buf, gz, index, _ = open_parts.pop(kind)
        gz.close()
        return kind, index, buf.getvalue()

    counters = {}
    for kind, record in records:
        if kind not in open_parts:
            buf = io.BytesIO()
            index = counters.get(kind, 0)
            counters[kind] = index + 1
            open_parts[kind] = [buf, gzip.GzipFile(fileobj=buf, mode="wb"), index, 0]
        part = open_parts[kind]
        part[1].write(json.dumps(record, ensure_ascii=False).encode("utf-8") + b"\n")
        part[3] += 1
        if part[3] >= rows_per_part:
            yield flush(kind)
    for kind in list(open_parts):
        yield flush(kind)
