import logging
from fastapi import FastAPI
import inngest
import inngest.fast_api
from inngest import experimental as ai 
from dotenv import load_dotenv
import uuid
import os
import datetime

from data_loader import load_and_chunk_pdf, embed_texts
from vector_db import QdrantStorage
from custom_types import RAGChunkAndSrc, RAGUpsertResult, RAGQueryResult, RAGSearchResult

load_dotenv()

inngest_client = inngest.Inngest(
    app_id="rag_app",
    logger=logging.getLogger("uvicorn"),
    is_production=False,
    serializer=inngest.PydanticSerializer(),
)

@inngest_client.create_function(
    fn_id="RAG: Ingest PDF",
    trigger=inngest.TriggerEvent(event="rag/ingest_pdf") # Added missing closing quote
)

async def rag_ingest_pdf(ctx: inngest.Context):
    def _load(ctx: inngest.Context) -> RAGChunkAndSrc:
        pdf_path = ctx.event.data["pdf_path"]
        source_id = ctx.event.data.get("source_id", pdf_path)
        chunks = load_and_chunk_pdf(pdf_path)
        return RAGChunkAndSrc(chunks=chunks, source_id=source_id)
        
    # Renamed input parameter to 'data' to match usage
    def _upsert(ctx: inngest.Context, data: RAGChunkAndSrc) -> RAGUpsertResult:
        # Use the 'data' parameter to access chunks and source_id
        chunks = data.chunks
        source_id = data.source_id # Define source_id locally
        vecs = embed_texts(chunks)
        ids = [str(uuid.uuid5(uuid.NAMESPACE_URL, f"{source_id}: {i}")) for i in range (len(chunks))]
        payloads = [{"source": source_id, "text": chunks[i]} for i in range (len(chunks))]
        QdrantStorage().upsert(ids, vecs, payloads)
        return RAGUpsertResult(ingested=len(chunks))
    
    # Execute the steps
    chunks_and_src = await ctx.step.run("load-and-chunk", lambda: _load(ctx), output_type=RAGChunkAndSrc)
    ingested = await ctx.step.run("embed-and-upsert", lambda: _upsert(ctx, chunks_and_src), output_type=RAGUpsertResult)
    return ingested.model_dump()

app = FastAPI()

# Typo fixed from inggest_client to inngest_client
inngest.fast_api.serve(app, inngest_client, [rag_ingest_pdf])
