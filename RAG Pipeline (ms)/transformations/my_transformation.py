"""
RAG Ingestion Pipeline — Lakeflow Declarative Pipeline source.

Defines three tables as a dependency chain:
  parsed_documents -> document_elements -> rag_document_chunks

Vector Search index creation/sync is NOT part of this pipeline — it's
an imperative SDK call against an external resource, not a DataFrame
transformation, so it runs as a separate Job task chained after this
pipeline's update completes.

Pipeline configuration (set in the pipeline's settings, not dbutils
.widgets — that's a notebook-only pattern, not used in pipeline source):
  pdf_volume_path  -> e.g. /Volumes/flight_tracker/raw/landing/rag_docs
"""

from pyspark import pipelines as dp
from pyspark.sql import functions as F
import pandas as pd

pdf_volume_path = spark.conf.get(
    "pdf_volume_path", ""
)

CONTENT_ELEMENT_TYPES = ["text", "title", "section_header", "caption", "table"]
MAX_CHUNK_WORDS = 1200  # safety cap so one very long section doesn't become a single giant chunk
CHUNK_OUTPUT_SCHEMA = "chunk_id string, source_document string, chunk_position int, section string, chunk_text string"


# ============================================================
# Table 1: parsed_documents
# ============================================================

@dp.table(
    comment="Raw ai_parse_document output for each PDF in the RAG corpus."
)
def parsed_documents():
    return spark.sql(f"""
        SELECT
            path,
            ai_parse_document(content, map('version', '2.0')) AS parsed
        FROM READ_FILES('{pdf_volume_path}/*', format => 'binaryFile')
    """)


# ============================================================
# Table 2: document_elements
# ============================================================

@dp.table(
    comment="Flattened, filtered content elements from each parsed document, "
            "keeping only real content types (dropping page headers/footers/numbers)."
)
def document_elements():
    parsed = dp.read("parsed_documents")
    parsed.createOrReplaceTempView("_parsed_documents_view")

    element_types_sql = ",".join(f"'{t}'" for t in CONTENT_ELEMENT_TYPES)
    return spark.sql(f"""
        SELECT
            path AS source_document,
            elem:id::int AS element_id,
            elem:type::string AS element_type,
            elem:content::string AS content
        FROM _parsed_documents_view
        LATERAL VIEW explode(
            CAST(parsed:document:elements AS ARRAY<VARIANT>)
        ) exploded AS elem
        WHERE elem:type::string IN ({element_types_sql})
    """)


# ============================================================
# Chunking helper — plain Python, NOT a pipeline table itself.
# Called from inside rag_document_chunks() below via applyInPandas.
# ============================================================

def chunk_by_section(pdf: pd.DataFrame) -> pd.DataFrame:
    """Runs once per document (one group = one source_document). Walks
    elements in order, starting a new chunk at every title/section_header,
    and sub-splitting only if a section's accumulated text exceeds
    MAX_CHUNK_WORDS."""
    pdf = pdf.sort_values("element_id")
    source_document = pdf["source_document"].iloc[0]

    chunks = []
    current_section = "Untitled"
    current_words: list[str] = []
    chunk_position = 0

    def flush_chunk():
        nonlocal chunk_position
        if not current_words:
            return
        for j in range(0, len(current_words), MAX_CHUNK_WORDS):
            sub_words = current_words[j: j + MAX_CHUNK_WORDS]
            chunks.append({
                "chunk_id": f"{source_document}::chunk_{chunk_position}",
                "source_document": source_document,
                "chunk_position": chunk_position,
                "section": current_section,
                "chunk_text": " ".join(sub_words),
            })
            chunk_position += 1

    for _, row in pdf.iterrows():
        if row["element_type"] in ("title", "section_header"):
            flush_chunk()
            current_words = []
            current_section = (row["content"] or "").strip() or current_section
            continue
        current_words.extend((row["content"] or "").split())

    flush_chunk()  # final section, if any content remains
    return pd.DataFrame(chunks)


# ============================================================
# Table 3: rag_document_chunks
# ============================================================

@dp.table(
    comment="Structure-aware chunks (split at section boundaries), ready "
            "for the Vector Search index. CDF is enabled via table "
            "properties since a STANDARD Vector Search endpoint requires it."
    ,
    table_properties={"delta.enableChangeDataFeed": "true"},
)
def rag_document_chunks():
    elements = dp.read("document_elements")
    return elements.groupBy("source_document").applyInPandas(
        chunk_by_section, schema=CHUNK_OUTPUT_SCHEMA
    )