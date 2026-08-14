use anyhow::Error;

use crate::{
    frame::{Frame, MAX_ARRAY_ELEMENTS, MAX_FRAME_BYTES},
    store::db::{Db, VectorQuantization, VectorSearchOptions, VectorSearchResult},
};

const MAX_VECTOR_DIMENSIONS: usize = 65_536;

fn validate_vector_response_count(count: usize, multiplier: usize) -> Result<(), Error> {
    if count > MAX_ARRAY_ELEMENTS / multiplier.max(1) {
        return Err(Error::msg("ERR count exceeds configured response limit"));
    }
    Ok(())
}

include!("command_types.rs");
include!("search_write_commands.rs");
include!("metadata_commands.rs");
include!("parse_helpers.rs");
include!("response_frames.rs");
