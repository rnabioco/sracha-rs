//! Native Vortex builders for the SRA → Vortex converter.
//!
//! Replaces the Arrow-intermediate path — rows go straight from VDB decode
//! into Vortex per-column builders, then into a `StructArray`. That makes the
//! choice of `DType::Utf8` vs `DType::List(u8)` explicit per column (so the
//! BtrBlocks cascade picks the right schemes for each shape) and removes the
//! `concat_batches` + Utf8-reinterpret round-trip the Arrow path needed.
//!
//! Schema (per-read, all columns always present):
//!
//! - `spot_id`: `u64`, non-nullable
//! - `read_num`: `u8`, non-nullable
//! - `name`: Utf8, nullable
//! - `read_len`: `u32`, non-nullable
//! - `sequence`: Utf8 when `pack_dna == Ascii` (FSST fires on the 4-letter
//!   alphabet), else `List<u8>` of packed bytes.
//!
//!   Vortex 0.68 hardcodes `VarBinView(Binary)` to bypass every compression
//!   scheme (see `Canonical::VarBinView` in
//!   `vortex-compressor::CascadingCompressor`). `List<u8>` sidesteps that
//!   bypass because `Canonical::List` recurses into its primitive child,
//!   which then goes through the numeric cascade (BitPack / Delta / RLE /
//!   Pco). On 2na-packed sequence this comes out denser than FSST-on-ASCII.
//! - `quality`: Utf8, nullable. Empirically best: FSST (via BtrBlocks'
//!   string cascade) dominates BitPack / Delta / Pco on quality Phred
//!   distributions. A `List<u8>` of raw Phred was tried and regressed by
//!   ~2× — the BtrBlocks numeric cascade doesn't pick an effective scheme
//!   on the primitive u8 child, while FSST's dictionary-of-substrings fits
//!   real quality-score patterns well. Keep it as text.

use std::sync::Arc;

use vortex::array::Array;
use vortex::array::arrays::{List, PrimitiveArray, StructArray};
use vortex::array::builders::{ArrayBuilder, PrimitiveBuilder, VarBinViewBuilder};
use vortex::array::validity::Validity;
use vortex::array::{ArrayRef, IntoArray};
use vortex::buffer::Buffer;
#[cfg(test)]
use vortex::dtype::PType;
use vortex::dtype::{DType, Nullability};

use crate::convert::decode::pack_sequence;
use crate::convert::schema::DnaPacking;
use crate::error::{Error, Result};

/// Row-oriented builder producing a single `StructArray` of the FASTQ columns.
pub(crate) struct VortexRowBuilder {
    spot_id: PrimitiveBuilder<u64>,
    read_num: PrimitiveBuilder<u8>,
    name: VarBinViewBuilder,
    read_len: PrimitiveBuilder<u32>,
    sequence: SequenceColumn,
    quality: VarBinViewBuilder,
    pack_dna: DnaPacking,
    rows: usize,
}

/// Sequence shape depends on DNA packing:
///   - `Ascii` → `VarBinView<Utf8>` (FSST trains on the 4-letter alphabet).
///   - `TwoNa` / `FourNa` → `List<u8>` so the packed-byte child array goes
///     through the numeric cascade instead of Vortex 0.68's Binary bypass.
#[allow(clippy::large_enum_variant)]
enum SequenceColumn {
    Utf8(VarBinViewBuilder),
    ListU8(ListU8Buffer),
}

/// Accumulates a non-nullable `List<Primitive(u8)>` column as flat element
/// bytes + Arrow-style cumulative offsets. Cheaper than going through
/// `ListViewBuilder` (which would allocate a temp `PrimitiveArray` per row
/// via `append_array_as_list`).
struct ListU8Buffer {
    elements: Vec<u8>,
    /// Cumulative, length == rows + 1. Seeded with `[0]`.
    offsets: Vec<u64>,
    rows: usize,
}

impl ListU8Buffer {
    fn with_capacity(row_capacity: usize, elem_capacity: usize) -> Self {
        let mut offsets = Vec::with_capacity(row_capacity + 1);
        offsets.push(0);
        Self {
            elements: Vec::with_capacity(elem_capacity),
            offsets,
            rows: 0,
        }
    }

    fn append_value(&mut self, bytes: &[u8]) {
        self.elements.extend_from_slice(bytes);
        self.offsets.push(self.elements.len() as u64);
        self.rows += 1;
    }

    fn finish(self) -> Result<ArrayRef> {
        let elements: ArrayRef =
            PrimitiveArray::new::<u8>(Buffer::from(self.elements), Validity::NonNullable)
                .into_array();
        let offsets: ArrayRef =
            PrimitiveArray::new::<u64>(Buffer::from(self.offsets), Validity::NonNullable)
                .into_array();
        let list: Array<List> = Array::<List>::try_new(elements, offsets, Validity::NonNullable)
            .map_err(|e| Error::Vdb(format!("vortex List<u8>: {e}")))?;
        Ok(list.into_array())
    }
}

impl VortexRowBuilder {
    pub(crate) fn with_capacity(pack_dna: DnaPacking, capacity: usize) -> Self {
        let sequence = match pack_dna {
            DnaPacking::Ascii => SequenceColumn::Utf8(VarBinViewBuilder::with_capacity(
                DType::Utf8(Nullability::NonNullable),
                capacity,
            )),
            _ => SequenceColumn::ListU8(ListU8Buffer::with_capacity(
                capacity,
                // Packed DNA is 0.25× (2na) or 0.5× (4na) of ASCII length;
                // assume ~40 bytes/row on typical 150bp Illumina as a rough
                // seed — Vec growth handles the rest.
                capacity.saturating_mul(40),
            )),
        };
        Self {
            spot_id: PrimitiveBuilder::with_capacity(Nullability::NonNullable, capacity),
            read_num: PrimitiveBuilder::with_capacity(Nullability::NonNullable, capacity),
            name: VarBinViewBuilder::with_capacity(DType::Utf8(Nullability::Nullable), capacity),
            read_len: PrimitiveBuilder::with_capacity(Nullability::NonNullable, capacity),
            sequence,
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

        match &mut self.sequence {
            SequenceColumn::Utf8(b) => b.append_value(sequence_ascii),
            SequenceColumn::ListU8(buf) => {
                let packed = pack_sequence(sequence_ascii, self.pack_dna);
                buf.append_value(&packed);
            }
        }

        if quality_ascii.is_empty() {
            self.quality.append_null();
        } else {
            self.quality.append_value(quality_ascii);
        }
        self.rows += 1;
    }

    pub(crate) fn finish(mut self) -> Result<ArrayRef> {
        let sequence = match self.sequence {
            SequenceColumn::Utf8(mut b) => b.finish(),
            SequenceColumn::ListU8(buf) => buf.finish()?,
        };
        let fields: Vec<(Arc<str>, ArrayRef)> = vec![
            (Arc::from("spot_id"), self.spot_id.finish()),
            (Arc::from("read_num"), self.read_num.finish()),
            (Arc::from("name"), self.name.finish()),
            (Arc::from("read_len"), self.read_len.finish()),
            (Arc::from("sequence"), sequence),
            (Arc::from("quality"), self.quality.finish()),
        ];
        let struct_arr = StructArray::try_from_iter(fields)
            .map_err(|e| Error::Vdb(format!("vortex struct array: {e}")))?;
        Ok(struct_arr.into_array())
    }
}

/// Element DType for the packed sequence / quality `List<u8>`, exposed for
/// tests that assert against the column shape.
#[cfg(test)]
pub(crate) fn list_u8_element_dtype() -> DType {
    DType::Primitive(PType::U8, Nullability::NonNullable)
}
