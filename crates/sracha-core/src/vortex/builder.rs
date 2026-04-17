//! Native Vortex builders for the SRA → Vortex converter.
//!
//! Replaces the Arrow-intermediate path — rows go straight from VDB decode
//! into Vortex per-column builders, then into a `StructArray`. That makes the
//! choice of `DType::Utf8` vs `DType::Binary` explicit per column (so Vortex's
//! BtrBlocks FSST/dict schemes fire for text-shaped data) and removes the
//! `concat_batches` + Utf8-reinterpret round-trip the Arrow path needed.
//!
//! Schema (per-read, all columns always present):
//!
//! - `spot_id`: `u64`, non-nullable
//! - `read_num`: `u8`, non-nullable
//! - `name`: Utf8, nullable
//! - `read_len`: `u32`, non-nullable
//! - `sequence`: Utf8 when `pack_dna == Ascii`, else Binary; non-nullable.
//!   Note: Vortex 0.68 hardcodes `VarBinView(Binary)` to bypass every
//!   compression scheme (see the `Canonical::VarBinView` arm in
//!   `vortex-compressor::CascadingCompressor`), so the 2na/4na-packed
//!   sequence is written uncompressed. Until that upstream restriction is
//!   lifted, prefer `--pack-dna ascii` for the smallest Vortex file —
//!   BtrBlocks' FSST cascade trains on the 4-letter alphabet effectively.
//! - `quality`: Utf8, nullable

use std::sync::Arc;

use vortex::array::arrays::StructArray;
use vortex::array::builders::{ArrayBuilder, PrimitiveBuilder, VarBinViewBuilder};
use vortex::array::{ArrayRef, IntoArray};
use vortex::dtype::{DType, Nullability};

use crate::convert::decode::pack_sequence;
use crate::convert::schema::DnaPacking;
use crate::error::{Error, Result};

pub(crate) struct VortexRowBuilder {
    spot_id: PrimitiveBuilder<u64>,
    read_num: PrimitiveBuilder<u8>,
    name: VarBinViewBuilder,
    read_len: PrimitiveBuilder<u32>,
    sequence: VarBinViewBuilder,
    quality: VarBinViewBuilder,
    pack_dna: DnaPacking,
    rows: usize,
}

impl VortexRowBuilder {
    pub(crate) fn with_capacity(pack_dna: DnaPacking, capacity: usize) -> Self {
        let sequence_dtype = match pack_dna {
            DnaPacking::Ascii => DType::Utf8(Nullability::NonNullable),
            _ => DType::Binary(Nullability::NonNullable),
        };
        Self {
            spot_id: PrimitiveBuilder::with_capacity(Nullability::NonNullable, capacity),
            read_num: PrimitiveBuilder::with_capacity(Nullability::NonNullable, capacity),
            name: VarBinViewBuilder::with_capacity(DType::Utf8(Nullability::Nullable), capacity),
            read_len: PrimitiveBuilder::with_capacity(Nullability::NonNullable, capacity),
            sequence: VarBinViewBuilder::with_capacity(sequence_dtype, capacity),
            quality: VarBinViewBuilder::with_capacity(DType::Utf8(Nullability::Nullable), capacity),
            pack_dna,
            rows: 0,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.rows == 0
    }

    pub(crate) fn push(
        &mut self,
        spot_id: u64,
        read_num: u8,
        name: &[u8],
        sequence_ascii: &[u8],
        quality_ascii: &[u8],
    ) {
        self.spot_id.append_value(spot_id);
        self.read_num.append_value(read_num);
        if name.is_empty() {
            self.name.append_null();
        } else {
            self.name.append_value(name);
        }
        self.read_len.append_value(sequence_ascii.len() as u32);
        let packed = pack_sequence(sequence_ascii, self.pack_dna);
        self.sequence.append_value(&packed);
        if quality_ascii.is_empty() {
            self.quality.append_null();
        } else {
            self.quality.append_value(quality_ascii);
        }
        self.rows += 1;
    }

    pub(crate) fn finish(mut self) -> Result<ArrayRef> {
        let fields: Vec<(Arc<str>, ArrayRef)> = vec![
            (Arc::from("spot_id"), self.spot_id.finish()),
            (Arc::from("read_num"), self.read_num.finish()),
            (Arc::from("name"), self.name.finish()),
            (Arc::from("read_len"), self.read_len.finish()),
            (Arc::from("sequence"), self.sequence.finish()),
            (Arc::from("quality"), self.quality.finish()),
        ];
        let struct_arr = StructArray::try_from_iter(fields)
            .map_err(|e| Error::Vdb(format!("vortex struct array: {e}")))?;
        Ok(struct_arr.into_array())
    }
}
