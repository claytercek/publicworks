use publicworks_runtime::*;
use publicworks_storage_sqlite::SqliteStorage;
use serde_json::{Value, json};
use std::{error::Error, path::Path};

const USAGE: &str = "Public Works: SQLite transcript and submission maintenance (Session, no agent)\n\
Usage:\n  publicworks create DB\n  publicworks append DB CONVERSATION TEXT\n  publicworks show DB CONVERSATION\n  publicworks submission DB SUBMISSION\n  publicworks abort-submission DB SUBMISSION [CONVERSATION]\n\
\nThe submission commands observe or withdraw existing durable receipts. They do not\nsubmit agent input, run providers, cancel placed work, or wait for task idleness.";

fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.as_slice() == ["--help"] || args.as_slice() == ["-h"] {
        println!("{USAGE}");
        return;
    }
    if args.as_slice() == ["--version"] || args.as_slice() == ["-V"] {
        println!("publicworks {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    match futures_lite::future::block_on(run(&args)) {
        Ok(output) => println!("{output}"),
        Err(error) => {
            eprintln!("publicworks: {error}");
            std::process::exit(1);
        }
    }
}

async fn run(args: &[String]) -> Result<Value, Box<dyn Error>> {
    let valid = matches!(args.first().map(String::as_str), Some("create") if args.len() == 2)
        || matches!(args.first().map(String::as_str), Some("append") if args.len() == 4)
        || matches!(args.first().map(String::as_str), Some("show" | "submission") if args.len() == 3)
        || matches!(args.first().map(String::as_str), Some("abort-submission") if args.len() == 3 || args.len() == 4);
    if !valid {
        return Err(USAGE.into());
    }
    if args[0] != "create" && !Path::new(&args[1]).is_file() {
        return Err("Database does not exist; use create first".into());
    }
    let (session, driver) = Session::new(SqliteStorage::open(&args[1])?);
    let command = async {
        let result = execute(&session, args).await;
        // Command failure must not skip shutdown. The host keeps polling the
        // driver until all admitted work and Storage.close have settled.
        let closed = session.close().await;
        let output = result?;
        closed?;
        Ok(output)
    };
    let (result, ()) = futures_lite::future::zip(command, driver).await;
    result
}

async fn execute(session: &Session, args: &[String]) -> Result<Value, Box<dyn Error>> {
    match args[0].as_str() {
        "create" => {
            let receipt = session
                .commit(|tx| Box::pin(async move { tx.create_conversation().await }))
                .await?;
            Ok(json!({"conversationId": receipt.value.id, "commitSeq": receipt.seq}))
        }
        "append" => {
            let conversation_id = Id::new(args[2].parse()?)?;
            let text = args[3].clone();
            let receipt = session
                .commit(move |tx| {
                    Box::pin(async move {
                        let mut draft = EntryDraft::new("publicworks.text");
                        draft.data = Some(json!({"text": text}));
                        tx.append_entry(conversation_id, draft).await
                    })
                })
                .await?;
            Ok(
                json!({"conversationId": conversation_id, "entryId": receipt.value.id, "commitSeq": receipt.seq}),
            )
        }
        "show" => {
            let conversation_id = Id::new(args[2].parse()?)?;
            let receipt = session
                .commit(move |tx| {
                    Box::pin(async move {
                        if tx.conversation(conversation_id).await?.is_none() {
                            return Err(SessionError::Invalid(format!(
                                "Unknown conversation: {conversation_id}"
                            )));
                        }
                        let mut entries = Vec::new();
                        let mut cursor = None;
                        loop {
                            let page = tx
                                .scan_entries(EntryQuery::new(conversation_id), 100, cursor)
                                .await?;
                            entries.extend(page.items);
                            cursor = page.next;
                            if cursor.is_none() {
                                break;
                            }
                        }
                        Ok(entries)
                    })
                })
                .await?;
            Ok(json!({"conversationId": conversation_id, "entries": receipt.value}))
        }
        "submission" => {
            let id = Id::new(args[2].parse()?)?;
            let record = session
                .commit(move |tx| {
                    Box::pin(async move {
                        tx.submission(id).await?.ok_or_else(|| {
                            SessionError::Invalid(format!("Unknown submission: {id}"))
                        })
                    })
                })
                .await?
                .value;
            Ok(serde_json::to_value(record)?)
        }
        "abort-submission" => {
            let id = Id::new(args[2].parse()?)?;
            let conversation = args
                .get(3)
                .map(|value| value.parse().map_err(Box::<dyn Error>::from))
                .transpose()?
                .map(Id::new)
                .transpose()?;
            let result = session
                .commit(move |tx| {
                    Box::pin(async move { tx.withdraw_submission(id, conversation).await })
                })
                .await?
                .value;
            let result = match result {
                WithdrawalResult::Aborted => "aborted",
                WithdrawalResult::AlreadyPlaced => "already_placed",
                WithdrawalResult::Settled => "settled",
                WithdrawalResult::NotFound => "not_found",
            };
            Ok(json!({"submissionId": id, "result": result}))
        }
        _ => unreachable!("arguments validated"),
    }
}
