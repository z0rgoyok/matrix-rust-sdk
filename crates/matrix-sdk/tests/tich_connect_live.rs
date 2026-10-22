//! Run only through Deploy OS's disposable-account fixture. No credentials in argv or logs.
#![recursion_limit = "256"]
#![cfg(feature = "tich-connect-transport")]

use matrix_sdk::{
    Client,
    attachment::AttachmentConfig,
    config::SyncSettings,
    media::{MediaFormat, MediaRequestParameters},
    ruma::{
        api::client::room::create_room::v3::Request, events::room::MediaSource,
        events::room::message::RoomMessageEventContent, serde::Raw,
    },
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    io::{self, Read},
    time::Duration,
};

#[derive(Deserialize)]
struct User {
    username: String,
    password: String,
}
#[derive(Deserialize)]
struct Fixture {
    users: Vec<User>,
}

async fn run(fixture: Fixture) -> Result<(), &'static str> {
    let mut clients = Vec::new();
    let mut syncs = Vec::new();
    for user in fixture.users {
        let client = Client::builder()
            .homeserver_url("https://connect.tich.app")
            .build()
            .await
            .map_err(|_| "build client")?;
        client
            .matrix_auth()
            .login_username(&user.username, &user.password)
            .initial_device_display_name("Tich native transport smoke")
            .await
            .map_err(|_| "login")?;
        let response =
            client.sync_once(SyncSettings::default()).await.map_err(|_| "initial sync")?;
        let syncing = client.clone();
        syncs.push(tokio::spawn(async move {
            let _ = syncing.sync(SyncSettings::default().token(response.next_batch)).await;
        }));
        clients.push(client);
    }
    let [alice, bob] = clients.as_slice() else { return Err("two fixture users required") };
    let mut request = Request::new();
    request.invite = vec![bob.user_id().ok_or("missing receiver")?.to_owned()];
    request.initial_state = vec![Raw::from_json_string(r#"{"type":"m.room.encryption","state_key":"","content":{"algorithm":"m.megolm.v1.aes-sha2"}}"#.to_owned()).map_err(|_| "room encryption")?];
    let room = alice.create_room(request).await.map_err(|_| "create encrypted room")?;
    println!("{}", serde_json::json!({"room":room.room_id()}));
    let receiver = bob.join_room_by_id(room.room_id()).await.map_err(|_| "join encrypted room")?;
    for _ in 0..30 {
        if room.latest_encryption_state().await.map_err(|_| "room encryption state")?.is_encrypted()
            && receiver
                .latest_encryption_state()
                .await
                .map_err(|_| "receiver encryption state")?
                .is_encrypted()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    if !room.latest_encryption_state().await.map_err(|_| "encryption state")?.is_encrypted() {
        return Err("room not encrypted");
    }
    let event = room
        .send(RoomMessageEventContent::text_plain("Tich native E2EE smoke"))
        .await
        .map_err(|_| "send encrypted message")?;
    let content = decrypted(&receiver, &event.response.event_id).await?;
    if content["body"] != "Tich native E2EE smoke" {
        return Err("message mismatch");
    }
    let payload = vec![0x59; 1024 * 1024];
    let event = room
        .send_attachment(
            "native-smoke.bin",
            &mime::APPLICATION_OCTET_STREAM,
            payload.clone(),
            AttachmentConfig::new(),
        )
        .await
        .map_err(|_| "upload encrypted file")?;
    let content = decrypted(&receiver, &event.event_id).await?;
    let file =
        serde_json::from_value(content["file"].clone()).map_err(|_| "file is not encrypted")?;
    let downloaded = bob
        .media()
        .get_media_content(
            &MediaRequestParameters {
                source: MediaSource::Encrypted(Box::new(file)),
                format: MediaFormat::File,
            },
            false,
        )
        .await
        .map_err(|_| "download encrypted file")?;
    if Sha256::digest(downloaded) != Sha256::digest(payload) {
        return Err("file digest mismatch");
    }
    for task in syncs {
        task.abort();
    }
    println!(
        "{}",
        serde_json::json!({"passed":true,"checks":{"native_login":"PASS",
        "native_e2ee_message":"PASS","native_encrypted_file_1MiB":"PASS","cdn":"PASS"}})
    );
    Ok(())
}

async fn decrypted(
    room: &matrix_sdk::Room,
    event: &ruma::EventId,
) -> Result<serde_json::Value, &'static str> {
    for _ in 0..30 {
        let received = room.event(event, None).await.map_err(|_| "receive event")?;
        let json: serde_json::Value =
            serde_json::from_str(received.raw().json().get()).map_err(|_| "decode event")?;
        if json["type"] == "m.room.message" {
            return Ok(json["content"].clone());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    Err("message decryption timeout")
}

#[tokio::test]
#[ignore = "requires disposable fixture accounts from Deploy OS on stdin"]
async fn native_e2ee_and_file() {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input).expect("fixture stdin unavailable");
    let fixture: Fixture =
        serde_json::from_str(&input).unwrap_or_else(|_| panic!("invalid fixture"));
    match tokio::time::timeout(Duration::from_secs(240), run(fixture)).await {
        Ok(Ok(())) => {}
        Ok(Err(stage)) => panic!("native smoke failed: {stage}"),
        Err(_) => panic!("native smoke deadline exceeded"),
    }
}
