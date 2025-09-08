//! This module exposes the in-memory store from Oxigraph as table in RdfFusion.
mod encoding;
mod object_id;
mod object_id_mapping;
mod planner;
mod storage;

pub use object_id_mapping::MemObjectIdMapping;
pub use storage::MemQuadStorage;
