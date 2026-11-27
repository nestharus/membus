use tantivy::schema::{Field, Schema, TantivyDocument, Value, INDEXED, STORED, STRING, TEXT};

use crate::search::overlay::SanitizedDoc;
use crate::search::trigram::extract_trigrams;
use crate::storage::StorageEngine;
use crate::types::MembusError;

pub(super) const SEARCH_CHECKPOINT_META_KEY: &[u8] = b"search:checkpoint";
pub(super) const WRITER_HEAP_BYTES: usize = 32 * 1024 * 1024;
pub(super) const MAX_SEARCH_LIMIT: usize = 1_000;
pub(super) const LOG_REPLAY_PAGE_LIMIT: usize = 512;

#[derive(Clone, Copy)]
pub(super) struct SearchSchemaFields {
    pub(super) doc_key: Field,
    pub(super) namespace: Field,
    pub(super) doc_id: Field,
    pub(super) kind: Field,
    pub(super) title: Field,
    pub(super) tags: Field,
    pub(super) tags_raw: Field,
    pub(super) body: Field,
    pub(super) trigrams: Field,
    pub(super) trigram_overflow: Field,
    pub(super) doc_seq: Field,
    pub(super) doc_op_idx: Field,
    pub(super) doc_epoch: Field,
}

pub(super) fn build_schema() -> (Schema, SearchSchemaFields) {
    let mut builder = Schema::builder();
    let doc_key = builder.add_text_field("doc_key", STRING | STORED);
    let namespace = builder.add_text_field("namespace", STRING | STORED);
    let doc_id = builder.add_text_field("doc_id", STRING | STORED);
    let kind = builder.add_text_field("kind", STRING | STORED);
    let title = builder.add_text_field("title", TEXT | STORED);
    let tags = builder.add_text_field("tags", TEXT);
    let tags_raw = builder.add_text_field("tags_raw", STORED);
    let body = builder.add_text_field("body", TEXT | STORED);
    let trigrams = builder.add_text_field("trigrams", STRING);
    let trigram_overflow = builder.add_u64_field("trigram_overflow", INDEXED | STORED);
    let doc_seq = builder.add_u64_field("doc_seq", INDEXED | STORED);
    let doc_op_idx = builder.add_u64_field("doc_op_idx", INDEXED | STORED);
    let doc_epoch = builder.add_u64_field("doc_epoch", STORED);
    let schema = builder.build();
    let fields = SearchSchemaFields {
        doc_key,
        namespace,
        doc_id,
        kind,
        title,
        tags,
        tags_raw,
        body,
        trigrams,
        trigram_overflow,
        doc_seq,
        doc_op_idx,
        doc_epoch,
    };
    (schema, fields)
}

pub(super) struct StoredSearchDoc {
    pub(super) doc_key: String,
    pub(super) namespace: String,
    pub(super) doc_id: String,
    pub(super) kind: String,
    pub(super) title: String,
    pub(super) tags: Vec<String>,
    pub(super) body: String,
    pub(super) doc_seq: u64,
    pub(super) doc_op_idx: u32,
}

pub(super) fn build_tantivy_document(
    sanitized: &SanitizedDoc,
    fields: SearchSchemaFields,
    seq: u64,
    op_idx: u32,
    epoch: u64,
) -> TantivyDocument {
    let trigrams = extract_trigrams(&[&sanitized.title, &sanitized.tags_joined, &sanitized.body]);

    let mut tdoc = TantivyDocument::default();
    tdoc.add_text(fields.doc_key, &sanitized.doc_key);
    tdoc.add_text(fields.namespace, &sanitized.namespace);
    tdoc.add_text(fields.doc_id, &sanitized.doc_id);
    tdoc.add_text(fields.kind, &sanitized.kind);
    tdoc.add_text(fields.title, &sanitized.title);
    tdoc.add_text(fields.tags, &sanitized.tags_joined);
    let tags_json = serde_json::to_string(&sanitized.tags).unwrap_or_else(|_| "[]".into());
    tdoc.add_text(fields.tags_raw, &tags_json);
    tdoc.add_text(fields.body, &sanitized.body);
    for gram in &trigrams.grams {
        tdoc.add_text(fields.trigrams, gram);
    }
    tdoc.add_u64(fields.trigram_overflow, u64::from(trigrams.overflow));
    tdoc.add_u64(fields.doc_seq, seq);
    tdoc.add_u64(fields.doc_op_idx, u64::from(op_idx));
    tdoc.add_u64(fields.doc_epoch, epoch);
    tdoc
}

pub(super) fn extract_stored_doc(
    doc: &TantivyDocument,
    fields: SearchSchemaFields,
) -> Option<StoredSearchDoc> {
    let doc_key = doc.get_first(fields.doc_key)?.as_str()?.to_owned();
    let namespace = doc.get_first(fields.namespace)?.as_str()?.to_owned();
    let doc_id = doc.get_first(fields.doc_id)?.as_str()?.to_owned();
    let kind = doc
        .get_first(fields.kind)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_owned();
    let title = doc
        .get_first(fields.title)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_owned();
    let tags: Vec<String> = doc
        .get_first(fields.tags_raw)
        .and_then(|v| v.as_str())
        .and_then(|raw| serde_json::from_str(raw).ok())
        .unwrap_or_default();
    let body = doc
        .get_first(fields.body)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_owned();
    let doc_seq = doc
        .get_first(fields.doc_seq)
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let doc_op_idx = doc
        .get_first(fields.doc_op_idx)
        .and_then(|v| v.as_u64())
        .and_then(|v| u32::try_from(v).ok())
        .unwrap_or(0);
    Some(StoredSearchDoc {
        doc_key,
        namespace,
        doc_id,
        kind,
        title,
        tags,
        body,
        doc_seq,
        doc_op_idx,
    })
}

pub(super) fn read_search_checkpoint(
    storage: &dyn StorageEngine,
) -> Result<Option<(u64, u64)>, MembusError> {
    let Some(bytes) = storage.meta_get(SEARCH_CHECKPOINT_META_KEY)? else {
        return Ok(None);
    };
    if bytes.len() != 16 {
        return Err(MembusError::Corruption(format!(
            "search checkpoint expected 16 bytes, got {}",
            bytes.len()
        )));
    }
    let epoch = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
    let seq = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
    Ok(Some((epoch, seq)))
}

pub(super) fn persist_search_checkpoint(
    storage: &dyn StorageEngine,
    epoch: u64,
    seq: u64,
) -> Result<(), MembusError> {
    let mut buf = [0u8; 16];
    buf[0..8].copy_from_slice(&epoch.to_be_bytes());
    buf[8..16].copy_from_slice(&seq.to_be_bytes());
    storage.meta_put(SEARCH_CHECKPOINT_META_KEY, &buf, true)
}

pub(super) fn map_tantivy_err(err: tantivy::TantivyError) -> MembusError {
    MembusError::Storage(format!("tantivy error: {err}"))
}
