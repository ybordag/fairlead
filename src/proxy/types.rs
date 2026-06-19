//! OpenAI-compatible wire types. Used by proxy handlers and tests;
//! structs not yet constructed in the binary will be live from Phase 3.
#![allow(dead_code)]

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
    #[serde(default)]
    pub stream: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCompletionResponse {
    pub id: String,
    pub object: String,
    pub model: String,
    pub choices: Vec<CompletionChoice>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionChoice {
    pub index: u32,
    pub message: ChatMessage,
    pub finish_reason: Option<String>,
}

/// One SSE frame sent when `stream: true`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCompletionChunk {
    pub id: String,
    pub object: String,
    pub model: String,
    pub choices: Vec<ChunkChoice>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkChoice {
    pub index: u32,
    pub delta: Delta,
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Delta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingRequest {
    pub model: String,
    pub input: EmbeddingInput,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EmbeddingInput {
    Single(String),
    Multiple(Vec<String>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingResponse {
    pub object: String,
    pub data: Vec<EmbeddingData>,
    pub model: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingData {
    pub object: String,
    pub index: u32,
    pub embedding: Vec<f32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chat_request_stream_defaults_false() {
        let json = r#"{"model":"m","messages":[{"role":"user","content":"hi"}]}"#;
        let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        assert!(!req.stream);
    }

    #[test]
    fn chat_request_stream_true() {
        let json = r#"{"model":"m","messages":[],"stream":true}"#;
        let req: ChatCompletionRequest = serde_json::from_str(json).unwrap();
        assert!(req.stream);
    }

    #[test]
    fn embedding_input_single() {
        let json = r#"{"model":"m","input":"hello"}"#;
        let req: EmbeddingRequest = serde_json::from_str(json).unwrap();
        assert!(matches!(req.input, EmbeddingInput::Single(_)));
    }

    #[test]
    fn embedding_input_multiple() {
        let json = r#"{"model":"m","input":["a","b"]}"#;
        let req: EmbeddingRequest = serde_json::from_str(json).unwrap();
        assert!(matches!(req.input, EmbeddingInput::Multiple(_)));
    }

    #[test]
    fn delta_skips_none_fields_in_serialization() {
        let delta = Delta {
            role: None,
            content: Some("hi".into()),
        };
        let json = serde_json::to_string(&delta).unwrap();
        assert!(!json.contains("role"), "null role should be omitted");
        assert!(json.contains("content"));
    }

    #[test]
    fn chat_completion_response_roundtrip() {
        let resp = ChatCompletionResponse {
            id: "chatcmpl-1".into(),
            object: "chat.completion".into(),
            model: "gpt-4".into(),
            choices: vec![CompletionChoice {
                index: 0,
                message: ChatMessage {
                    role: "assistant".into(),
                    content: "Hello!".into(),
                },
                finish_reason: Some("stop".into()),
            }],
        };
        let json = serde_json::to_string(&resp).unwrap();
        let back: ChatCompletionResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(back.choices[0].message.content, "Hello!");
    }
}
