//! Arrow schema construction for SRA → Parquet output.
//!
//! The choice of DNA packing and length mode lives in
//! [`crate::convert::schema`]; this module maps those choices to an actual
//! Arrow `Schema` that Parquet's `ArrowWriter` understands.
//!
//! Two schema variants are supported, chosen at runtime from the data:
//!
//! * [`LengthMode::Fixed`] — every read in the run has the same length, so
//!   `sequence` and `quality` are emitted as `FIXED_LEN_BYTE_ARRAY` with the
//!   length declared once in the schema. No per-row length prefix.
//! * [`LengthMode::Variable`] — read lengths vary, so `sequence` and `quality`
//!   are `BYTE_ARRAY` and the actual length is recorded in `read_len`.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};

use crate::convert::schema::{DnaPacking, LengthMode};

/// Build the per-read Arrow schema for the chosen length mode and DNA packing.
///
/// Schema layout (per-read rows):
///
/// | column      | type                              | notes                          |
/// |-------------|-----------------------------------|--------------------------------|
/// | spot_id     | UInt64                            | parent spot identifier         |
/// | read_num    | UInt8                             | 0-based read index within spot |
/// | name        | Utf8 (nullable)                   | spot name                      |
/// | read_len    | UInt32 (variable mode only)       | omitted in fixed mode          |
/// | sequence    | (FixedSize)Binary                 | width depends on mode + packing|
/// | quality     | (FixedSize)Binary (nullable)      | width depends on mode          |
pub fn build_per_read_schema(length_mode: LengthMode, packing: DnaPacking) -> SchemaRef {
    let mut fields: Vec<Field> = Vec::with_capacity(6);
    fields.push(Field::new("spot_id", DataType::UInt64, false));
    fields.push(Field::new("read_num", DataType::UInt8, false));
    fields.push(Field::new("name", DataType::Utf8, true));

    match length_mode {
        LengthMode::Fixed { read_len } => {
            let seq_bytes = packing.packed_len(read_len) as i32;
            fields.push(Field::new(
                "sequence",
                DataType::FixedSizeBinary(seq_bytes),
                false,
            ));
            fields.push(Field::new(
                "quality",
                DataType::FixedSizeBinary(read_len as i32),
                true,
            ));
        }
        LengthMode::Variable => {
            fields.push(Field::new("read_len", DataType::UInt32, false));
            fields.push(Field::new("sequence", DataType::Binary, false));
            fields.push(Field::new("quality", DataType::Binary, true));
        }
    }

    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_mode_omits_read_len_column() {
        let schema = build_per_read_schema(LengthMode::Fixed { read_len: 150 }, DnaPacking::Ascii);
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(!names.contains(&"read_len"));
        assert!(names.contains(&"sequence"));
        assert!(names.contains(&"quality"));
    }

    #[test]
    fn variable_mode_includes_read_len_column() {
        let schema = build_per_read_schema(LengthMode::Variable, DnaPacking::Ascii);
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(names.contains(&"read_len"));
    }

    #[test]
    fn fixed_2na_uses_packed_width() {
        let schema = build_per_read_schema(LengthMode::Fixed { read_len: 150 }, DnaPacking::TwoNa);
        let seq_field = schema.field_with_name("sequence").unwrap();
        assert!(matches!(
            seq_field.data_type(),
            DataType::FixedSizeBinary(38)
        ));
    }

    #[test]
    fn fixed_ascii_uses_unpacked_width() {
        let schema = build_per_read_schema(LengthMode::Fixed { read_len: 150 }, DnaPacking::Ascii);
        let seq_field = schema.field_with_name("sequence").unwrap();
        assert!(matches!(
            seq_field.data_type(),
            DataType::FixedSizeBinary(150)
        ));
    }

    #[test]
    fn quality_width_matches_read_len_in_fixed_mode() {
        let schema = build_per_read_schema(LengthMode::Fixed { read_len: 75 }, DnaPacking::TwoNa);
        let qual_field = schema.field_with_name("quality").unwrap();
        assert!(matches!(
            qual_field.data_type(),
            DataType::FixedSizeBinary(75)
        ));
    }
}
