fn indexed_filter_field<'a>(
    meta: &'a VectorIndexMeta,
    predicate: &FilterPredicate,
) -> Option<&'a str> {
    let (field_name, expected_kind) = match predicate {
        FilterPredicate::TagEq(field, _) | FilterPredicate::TagIn(field, _) => {
            (field.as_str(), VectorFieldKind::Tag)
        }
        FilterPredicate::NumericCmp(field, _, _) => (field.as_str(), VectorFieldKind::Numeric),
    };
    meta.schema
        .iter()
        .find(|field| field.indexed && field.name == field_name && field.kind == expected_kind)
        .map(|field| field.name.as_str())
}

struct VectorAttrIndexContext<'a> {
    db_index: u16,
    index: &'a str,
    version: u64,
    schema: &'a [VectorFieldSchema],
    doc_id: &'a str,
}

fn put_attr_index_entries_to_batch(
    batch: &mut WriteBatch,
    context: &VectorAttrIndexContext<'_>,
    doc_version: u64,
    attrs: &JsonValue,
) -> Result<(), Error> {
    for field in context.schema.iter().filter(|field| field.indexed) {
        let Some(value) = attrs.get(&field.name) else {
            continue;
        };
        match field.kind {
            VectorFieldKind::Tag => {
                for tag in tag_values(value)? {
                    batch.put(
                        &vector_tag_key(
                            context.db_index,
                            context.index,
                            context.version,
                            &field.name,
                            &tag,
                            context.doc_id,
                        ),
                        &doc_version.to_be_bytes(),
                    );
                }
            }
            VectorFieldKind::Numeric => {
                if let Some(number) = value.as_f64() {
                    batch.put(
                        &vector_numeric_key(
                            context.db_index,
                            context.index,
                            context.version,
                            &field.name,
                            number,
                            context.doc_id,
                        ),
                        &doc_version.to_be_bytes(),
                    );
                }
            }
            VectorFieldKind::Text => {}
        }
    }
    Ok(())
}

fn delete_attr_index_entries_to_batch(
    batch: &mut WriteBatch,
    context: &VectorAttrIndexContext<'_>,
    attrs: &JsonValue,
) {
    for field in context.schema.iter().filter(|field| field.indexed) {
        let Some(value) = attrs.get(&field.name) else {
            continue;
        };
        match field.kind {
            VectorFieldKind::Tag => {
                if let Ok(tags) = tag_values(value) {
                    for tag in tags {
                        batch.delete(&vector_tag_key(
                            context.db_index,
                            context.index,
                            context.version,
                            &field.name,
                            &tag,
                            context.doc_id,
                        ));
                    }
                }
            }
            VectorFieldKind::Numeric => {
                if let Some(number) = value.as_f64() {
                    batch.delete(&vector_numeric_key(
                        context.db_index,
                        context.index,
                        context.version,
                        &field.name,
                        number,
                        context.doc_id,
                    ));
                }
            }
            VectorFieldKind::Text => {}
        }
    }
}

fn tag_values(value: &JsonValue) -> Result<Vec<String>, Error> {
    if let Some(text) = value.as_str() {
        return Ok(vec![text.to_string()]);
    }
    if let Some(values) = value.as_array() {
        let mut tags = Vec::with_capacity(values.len());
        for value in values {
            let Some(text) = value.as_str() else {
                return Err(Error::msg("ERR vector tag array must contain strings"));
            };
            tags.push(text.to_string());
        }
        return Ok(tags);
    }
    Err(Error::msg("ERR vector tag field must be string or array"))
}
