//! MCAP file format support.
//!
//! Provides reading, writing, and rewriting of MCAP files.

pub mod reader;
pub mod rewrite_engine;
pub mod rewriter;
pub mod transform;

pub use reader::{
    ChannelInfo, DecodedMessageIter, DecodedMessageStream, DecodedMessageWithTimestampIter,
    DecodedMessageWithTimestampStream, McapReader, RawMessage, RawMessageIter, RawMessageStream,
    TimestampedDecodedMessage,
};

pub use rewrite_engine::{McapRewriteEngine, McapRewriteStats};
pub use rewriter::McapRewriter;
// Re-export shared types for convenience
pub use crate::rewriter::{FormatRewriter, RewriteOptions, RewriteStats};
pub use transform::{
    McapTransform, TopicAwareTypeRenameTransform, TopicRenameTransform, TransformBuilder,
    TransformError, TransformPipeline, TransformedChannel, TypeNormalization, TypeRenameTransform,
};
