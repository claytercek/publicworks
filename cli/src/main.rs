use publicworks_runtime::*;
use publicworks_storage_sqlite::SqliteStorage;
use serde_json::{Value, json};
use std::{error::Error, path::Path};

const USAGE: &str = "Public Works: low-level transcript/storage demo (no Session or agent)\n\
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
    let mut adapter = SqliteStorage::open(&args[1])?;
    let store: &mut dyn Storage = &mut adapter;
    let output = if let Some(conversation_id) = conversation {
        if store.conversation(conversation_id).await?.is_none() {
            return Err(format!("Unknown conversation: {conversation_id}").into());
        }
        if command == "append" {
            let id = store.mint_id().await?;
            let mut entry = EntryRecord::new(id, conversation_id, "publicworks.text");
            entry.data = Some(json!({"text": args[3]}));
            let seq = store.commit(vec![StorageWrite::Entry(entry)]).await?;
            json!({"conversationId": conversation_id, "entryId": id, "commitSeq": seq})
        } else {
            let mut entries = Vec::new();
            let mut cursor = None;
            loop {
                let page = store
                    .scan_entries(EntryQuery::new(conversation_id), 100, cursor)
                    .await?;
                entries.extend(page.items);
                cursor = page.next;
                if cursor.is_none() {
                    break;
                }
            }
            json!({"conversationId": conversation_id, "entries": entries})
        }
    } else {
        let id = store.mint_id().await?;
        let record = ConversationRecord {
            id,
            parent: None,
            owner: None,
        };
        let seq = store
            .commit(vec![StorageWrite::Conversation(record)])
            .await?;
        json!({"conversationId": id, "commitSeq": seq})
    };
    store.close().await?;
    Ok(output)
}
