//! LIVE example: run one durable OpenAI Responses turn with a local calculator.
//!
//! This adapter intentionally rejects reasoning output. Use a non-reasoning model
//! such as the default `gpt-4.1-mini` until reasoning items can be durably replayed.
use futures_lite::future::zip;
use publicworks_agent::{
    Agent, ModelMessage, SubmitOptions, Tool, ToolDeclaration, ToolResult, TurnConfig,
    decode_message,
};
use publicworks_provider_openai::OpenAiResponses;
use publicworks_runtime::{Harness, MemoryStorage, SubmissionState, TaskRegistry};
use serde_json::{Value, json};
use std::{error::Error, io};

fn calculator() -> Tool {
    let declaration = ToolDeclaration {
        name: "calculator".into(),
        version: 1,
        description: "Add, subtract, or multiply two signed integers safely".into(),
        parameters: json!({
            "type":"object",
            "properties":{
                "operation":{"type":"string","enum":["add","subtract","multiply"]},
                "left":{"type":"integer"},
                "right":{"type":"integer"}
            },
            "required":["operation","left","right"],
            "additionalProperties":false
        }),
    };
    Tool::new(
        declaration,
        |arguments| {
            let object = arguments
                .as_object()
                .ok_or_else(|| "calculator arguments must be an object".to_owned())?;
            let operation = object
                .get("operation")
                .and_then(Value::as_str)
                .ok_or_else(|| "operation must be a string".to_owned())?;
            if !matches!(operation, "add" | "subtract" | "multiply") {
                return Err("unsupported calculator operation".into());
            }
            for field in ["left", "right"] {
                if object.get(field).and_then(Value::as_i64).is_none() {
                    return Err(format!("{field} must be a signed integer"));
                }
            }
            Ok(())
        },
        |call, _| {
            Box::pin(async move {
                let operation = call.arguments["operation"].as_str().unwrap();
                let left = call.arguments["left"].as_i64().unwrap();
                let right = call.arguments["right"].as_i64().unwrap();
                let result = match operation {
                    "add" => left.checked_add(right),
                    "subtract" => left.checked_sub(right),
                    "multiply" => left.checked_mul(right),
                    _ => unreachable!("validator rejected unknown operation"),
                };
                Ok(ToolResult {
                    content: result
                        .map(|value| value.to_string())
                        .unwrap_or_else(|| "integer overflow".into()),
                    is_error: result.is_none(),
                    usage: None,
                })
            })
        },
    )
}

fn main() -> Result<(), Box<dyn Error>> {
    let api_key = std::env::var("OPENAI_API_KEY").map_err(|_| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "OPENAI_API_KEY is required for this LIVE example",
        )
    })?;
    let model = std::env::var("OPENAI_MODEL").unwrap_or_else(|_| "gpt-4.1-mini".into());
    let prompt = {
        let supplied = std::env::args().skip(1).collect::<Vec<_>>().join(" ");
        if supplied.is_empty() {
            "Use the calculator to multiply 123 by 456, then answer briefly.".into()
        } else {
            supplied
        }
    };

    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run(api_key, model, prompt))
}

async fn run(api_key: String, model: String, prompt: String) -> Result<(), Box<dyn Error>> {
    // The credential is captured only by the provider. It is never put in the
    // durable turn configuration or task input.
    let provider = OpenAiResponses::new(api_key)?;
    let agent = Agent::new(provider, vec![calculator()])?;
    let (opening, driver) = Harness::open(
        MemoryStorage::new(),
        TaskRegistry::new(agent.definitions())?,
    );
    let work = async {
        let harness = opening.await?;
        let result = execute_turn(&harness, agent, model, prompt).await;
        harness.close().await?;
        result
    };
    let (result, ()) = zip(work, driver).await;
    println!("{}", result?);
    Ok(())
}

async fn execute_turn(
    harness: &Harness,
    agent: Agent,
    model: String,
    prompt: String,
) -> Result<String, Box<dyn Error>> {
    let conversation = harness
        .commit(|tx| Box::pin(async move { tx.create_conversation().await }))
        .await?
        .value
        .id;
    let submission = agent
        .submit(
            harness,
            conversation,
            prompt,
            TurnConfig {
                model,
                instructions: "Use the calculator for arithmetic and answer briefly.".into(),
                max_model_rounds: 3,
            },
            SubmitOptions::default(),
        )
        .await?;
    let receipt = submission.wait().await?;
    let SubmissionState::InputDone { answer, .. } = receipt.state else {
        return Err(io::Error::other(format!("submission unanswered: {:?}", receipt.state)).into());
    };
    let entry = harness
        .commit(move |tx| {
            Box::pin(
                async move { Ok(tx.visible_entry(conversation, answer).await?.unwrap().entry) },
            )
        })
        .await?
        .value;
    entry
        .model
        .as_ref()
        .and_then(|messages| messages.first())
        .map(decode_message)
        .transpose()?
        .and_then(|message| match message {
            ModelMessage::Assistant { text, .. } => Some(text),
            _ => None,
        })
        .ok_or_else(|| io::Error::other("turn completed without an assistant answer").into())
}
