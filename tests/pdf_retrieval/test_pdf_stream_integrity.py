# ruff: noqa: S106, S311
"""Direct page-stream integrity and compatibility through the public parser."""

import hashlib
import io
import zlib
from concurrent.futures import ThreadPoolExecutor

import pytest
from pypdf import PdfWriter
from pypdf.generic import (
    ArrayObject,
    DecodedStreamObject,
    DictionaryObject,
    EncodedStreamObject,
    NameObject,
    NullObject,
)

from powercontext.sources.pdf_pages import PDFDocumentInput, parse_pdf_document


def make_pdf(mode="normal", *, contents_array=False, password=None):  # noqa: C901
    writer = PdfWriter()
    font = DictionaryObject({
        NameObject("/Type"): NameObject("/Font"),
        NameObject("/Subtype"): NameObject("/Type1"),
        NameObject("/BaseFont"): NameObject("/Helvetica"),
    })
    for index, text in enumerate((b"First page revenue 123", b"Second page evidence 987")):
        page = writer.add_blank_page(width=600, height=800)
        page[NameObject("/Resources")] = DictionaryObject({
            NameObject("/Font"): DictionaryObject({NameObject("/F1"): writer._add_object(font)})
        })
        payload = b"BT /F1 12 Tf 20 700 Td (" + text + b") Tj ET"
        stream = EncodedStreamObject()
        stream[NameObject("/Filter")] = NameObject("/FlateDecode")
        stream._data = zlib.compress(payload)
        if index == 1:
            if mode == "header":
                stream._data = b"x" * len(stream._data)
            elif mode == "truncated":
                stream._data = stream._data[:-4]
            elif mode == "checksum":
                stream._data = stream._data[:-1] + bytes([stream._data[-1] ^ 1])
            elif mode == "trailing":
                stream._data += b"ordinary trailing bytes"
            elif mode == "abbreviation":
                stream[NameObject("/Filter")] = NameObject("/Fl")
            elif mode == "single-filter-array":
                stream[NameObject("/Filter")] = ArrayObject([NameObject("/FlateDecode")])
        ref = writer._add_object(stream)
        if contents_array:
            prefix = DecodedStreamObject()
            prefix.set_data(b"q Q")
            page[NameObject("/Contents")] = ArrayObject([writer._add_object(prefix), ref])
        else:
            page[NameObject("/Contents")] = ref
    if password is not None:
        writer.encrypt(user_password=password, owner_password="owner")
    buffer = io.BytesIO()
    writer.write(buffer)
    return buffer.getvalue()


def parse(raw):
    return parse_pdf_document(PDFDocumentInput(document_id="stream-contract", content=raw))


@pytest.mark.parametrize("mode", ["header", "truncated", "checksum"])
@pytest.mark.parametrize("contents_array", [False, True])
def test_damaged_later_stream_fails_atomically(mode, contents_array):
    with pytest.raises(ValueError):
        parse(make_pdf(mode, contents_array=contents_array))


@pytest.mark.parametrize("mode", ["normal", "trailing", "single-filter-array"])
@pytest.mark.parametrize("contents_array", [False, True])
def test_valid_compressed_streams_preserve_pages(mode, contents_array):
    raw = make_pdf(mode, contents_array=contents_array)
    pages = parse(raw)
    assert [page.text.strip() for page in pages] == ["First page revenue 123", "Second page evidence 987"]
    assert [page.page_number for page in pages] == [1, 2]
    assert all(page.document_sha256 == hashlib.sha256(raw).hexdigest() for page in pages)


def test_filter_abbreviation_warning_is_not_an_error():
    with pytest.warns(DeprecationWarning, match="filter name /Fl"):
        assert len(parse(make_pdf("abbreviation"))) == 2


def test_blank_content_variants_remain_blank():
    writer = PdfWriter()
    for mode in ("missing", "null", "array", "decoded", "compressed"):
        page = writer.add_blank_page(width=600, height=800)
        if mode == "null":
            page[NameObject("/Contents")] = NullObject()
        elif mode == "array":
            page[NameObject("/Contents")] = ArrayObject()
        elif mode in ("decoded", "compressed"):
            stream = DecodedStreamObject()
            stream.set_data(b"")
            if mode == "compressed":
                stream = stream.flate_encode()
            page[NameObject("/Contents")] = writer._add_object(stream)
    buffer = io.BytesIO()
    writer.write(buffer)
    pages = parse(buffer.getvalue())
    assert len(pages) == 5
    assert all(page.text == "" for page in pages)


def test_permissions_are_decrypted_before_validation():
    raw = make_pdf(password="")
    assert [p.text for p in parse(raw)] == [p.text for p in parse(make_pdf())]
    assert all(p.document_sha256 == hashlib.sha256(raw).hexdigest() for p in parse(raw))
    with pytest.raises(ValueError):
        parse(make_pdf(password="secret"))


def test_parallel_readers_do_not_share_failure_state():
    good, bad, encrypted = make_pdf(), make_pdf("header"), make_pdf(password="")

    def outcome(raw):
        try:
            return len(parse(raw))
        except ValueError:
            return "invalid"

    with ThreadPoolExecutor(max_workers=4) as pool:
        result = list(pool.map(outcome, [good, bad, encrypted] * 8))
    assert result == [2, "invalid", 2] * 8


@pytest.mark.parametrize("payload", [b"ledger " * 50000, bytes(range(256)) * 800])
def test_preflight_output_limit_across_chunks(payload):
    from powercontext.sources.pdf_pages import _validate_flate

    encoded = zlib.compress(payload)
    _validate_flate(encoded, len(payload))
    _validate_flate(encoded, 0)
    with pytest.raises(ValueError, match="output limit"):
        _validate_flate(encoded, len(payload) - 1)


def test_preflight_handles_multiple_input_chunks():
    import random

    from powercontext.sources.pdf_pages import _validate_flate

    payload = random.Random(3701).randbytes(200000)
    _validate_flate(zlib.compress(payload), len(payload))
