//! Add missing native commands without replacing the app owner's entries.

use serde_json::{Value, json};

use super::api::{Api, ApiError};

// https://open.feishu.cn/document/server-docs/application-v7/app_slash_command/list
const COMMANDS: &str = "/open-apis/application/v7/app_slash_commands";
const COMMON: &[(&str, &str)] = &[
    ("new", "Start a new session and keep the previous history"),
    ("stop", "Stop the current turn"),
    (
        "compact",
        "Compact the context; optionally say what to keep",
    ),
    ("status", "Show the current session, model and effort"),
    ("model", "Show or change the model"),
    ("think", "Show or change the thinking level"),
    ("help", "Show common commands and their usage"),
];

pub(super) async fn sync(api: &Api) -> Result<(), ApiError> {
    let listed = api.get(COMMANDS).await?;
    let items = listed["data"]["items"]
        .as_array()
        .ok_or_else(|| ApiError::Transport("feishu slash-command list has no items".into()))?;
    // A partial or malformed catalogue cannot prove that a name is absent.
    if listed["data"]["has_more"].as_bool() == Some(true)
        || items.iter().any(|item| item["command"].as_str().is_none())
    {
        return Err(ApiError::Transport(
            "feishu slash-command list is incomplete".into(),
        ));
    }
    for &(name, description) in COMMON {
        if !items
            .iter()
            .any(|item| item["command"].as_str() == Some(name))
        {
            api.post(COMMANDS, body(name, description)).await?;
        }
    }
    Ok(())
}

fn body(name: &str, description: &str) -> Value {
    json!({
        "command": name,
        "description": { "default_value": description }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn api(server: &MockServer) -> Api {
        Mock::given(method("POST"))
            .and(path("/open-apis/auth/v3/tenant_access_token/internal"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "code": 0, "tenant_access_token": "fixture", "expire": 7200
            })))
            .mount(server)
            .await;
        Api::new(server.uri(), "cli_fixture", "fixture")
    }

    async fn catalogue(server: &MockServer, response: Value) {
        Mock::given(method("GET"))
            .and(path(COMMANDS))
            .respond_with(ResponseTemplate::new(200).set_body_json(response))
            .mount(server)
            .await;
    }

    #[test]
    fn the_catalogue_uses_unique_names_and_english_descriptions() {
        let names: std::collections::BTreeSet<_> = COMMON.iter().map(|(name, _)| *name).collect();
        assert_eq!(names.len(), COMMON.len());
        assert_eq!(
            names,
            ["new", "stop", "compact", "status", "model", "think", "help"].into()
        );
        assert_eq!(
            body("new", "Start a new session"),
            json!({
                "command": "new", "description": { "default_value": "Start a new session" }
            })
        );
        assert!(COMMON.iter().all(|(_, description)| description.is_ascii()));
    }

    #[tokio::test]
    async fn registration_adds_only_missing_names_and_preserves_owned_entries() {
        let server = MockServer::start().await;
        catalogue(
            &server,
            json!({
                "code": 0, "data": { "items": [
                    { "command": "compact", "description": { "default_value": "My description" } },
                    { "command": "custom" }
                ], "has_more": false }
            }),
        )
        .await;
        for &(name, description) in COMMON.iter().filter(|(name, _)| *name != "compact") {
            Mock::given(method("POST"))
                .and(path(COMMANDS))
                .and(body_json(body(name, description)))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "code": 0, "data": { "command_id": name }
                })))
                .expect(1)
                .mount(&server)
                .await;
        }
        sync(&api(&server).await).await.expect("registration");
        let requests = server.received_requests().await.expect("requests");
        assert_eq!(
            requests
                .iter()
                .filter(|r| r.url.path() == COMMANDS && r.method == "POST")
                .count(),
            COMMON.len() - 1
        );
        assert!(
            !requests
                .iter()
                .any(|r| r.method == "PATCH" || r.method == "DELETE" || r.method == "PUT")
        );
    }

    #[tokio::test]
    async fn a_complete_catalogue_needs_no_writes() {
        let server = MockServer::start().await;
        catalogue(&server, json!({
            "code": 0, "data": { "items": COMMON.iter().map(|(name, _)| json!({ "command": name })).collect::<Vec<_>>() }
        })).await;
        sync(&api(&server).await).await.expect("registration");
        let requests = server.received_requests().await.expect("requests");
        assert_eq!(
            requests.iter().filter(|r| r.url.path() == COMMANDS).count(),
            1
        );
    }

    #[tokio::test]
    async fn an_incomplete_malformed_or_refused_catalogue_is_not_modified() {
        for response in [
            json!({ "code": 99991672, "msg": "missing scope" }),
            json!({ "code": 0, "data": { "items": [], "has_more": true } }),
            json!({ "code": 0, "data": {} }),
            json!({ "code": 0, "data": { "items": [{}] } }),
        ] {
            let server = MockServer::start().await;
            catalogue(&server, response).await;
            assert!(sync(&api(&server).await).await.is_err());
            let requests = server.received_requests().await.expect("requests");
            assert_eq!(
                requests.iter().filter(|r| r.url.path() == COMMANDS).count(),
                1
            );
        }
    }

    #[tokio::test]
    async fn a_refused_creation_stops_without_rewriting_the_catalogue() {
        let server = MockServer::start().await;
        catalogue(&server, json!({ "code": 0, "data": { "items": [] } })).await;
        Mock::given(method("POST"))
            .and(path(COMMANDS))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "code": 99991672, "msg": "missing write scope"
            })))
            .expect(1)
            .mount(&server)
            .await;
        assert!(sync(&api(&server).await).await.is_err());
    }
}
