//! Protobuf codec module.
//!
//! Provides Protobuf decoding support.

pub mod codec;
pub mod decoder;

pub use codec::ProtobufCodec;
pub use decoder::ProtobufDecoder;
