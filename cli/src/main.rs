use publicworks_runtime::*;
use publicworks_storage_sqlite::SqliteStorage;
use serde_json::{Value, json};
use std::{error::Error, path::Path};

const USAGE: &str = "Public Works: transactional transcript demo (Session, no agent)\n\
Usage:\n  publicworks create DB\n  publicworks append DB CONVERSATION TEXT\n  publicworks show DB CONVERSATION";

fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.as_slice() == ["--help"] || args.as_slice() == ["-h"] {
        println!("{USAGE}");
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
        || matches!(args.first().map(String::as_str), Some("show") if args.len() == 3);
    if !valid {
        return Err(USAGE.into());
    }
    let command = args[0].as_str();
    let conversation = if command == "create" {
        None
    } else {
        Some(Id::new(args[2].parse()?)?)
    };
    if command != "create" && !Path::new(&args[1]).is_file() {
        return Err("Database does not exist; use create first".into());
    }
    let (session, driver) = Session::new(SqliteStorage::open(&args[1])?);
    let command = async {
        let result = execute(&session, command, conversation, args.get(3).cloned()).await;
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

async fn execute(
    session: &Session,
    command: &str,
    conversation: Option<Id>,
    text: Option<String>,
) -> Result<Value, Box<dyn Error>> {
    if let Some(conversation_id) = conversation {
        if command == "append" {
            let receipt = session
                .commit(move |tx| {
                    Box::pin(async move {
                        let mut draft = EntryDraft::new("publicworks.text");
                        draft.data =
                            Some(json!({"text": text.expect("append arguments validated")}));
                        tx.append_entry(conversation_id, draft).await
                    })
                })
                .await?;
            Ok(
                json!({"conversationId": conversation_id, "entryId": receipt.value.id, "commitSeq": receipt.seq}),
            )
        } else {
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
    } else {
        let receipt = session
            .commit(|tx| Box::pin(async move { tx.create_conversation().await }))
            .await?;
        Ok(json!({"conversationId": receipt.value.id, "commitSeq": receipt.seq}))
    }
}
