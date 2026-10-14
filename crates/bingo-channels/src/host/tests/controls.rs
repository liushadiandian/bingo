use super::*;

#[tokio::test]
async fn help_is_a_surface_reply_not_a_model_prompt() {
    let chat = Chat::open();
    chat.say(said(Conversation::direct("oc_1"), "/help", true))
        .await;
    let session = chat.session("loopback/oc_1").await;
    let records = chat.records(1).await;
    assert!(session.prompts().is_empty());
    assert!(records.iter().any(|record| matches!(record,
        Record::Send { text, mode: Mode::Once, .. }
            if text.starts_with("/new - ") && text.contains("/compact [instructions]")
                && text.contains("/help - ")
    )));
}

async fn switched(chat: &Chat) -> Vec<Record> {
    chat.until(|| {
        let records = chat.loopback.records();
        records
            .iter()
            .any(|record| {
                matches!(record,
                    Record::Send { text, .. } | Record::Reply { text, .. }
                        if text == "新会话已开启。"
                )
            })
            .then_some(records)
    })
    .await
}

fn streaming(session: &TestSession) {
    session.publish(Event::TurnStarted {
        turn: bingo_sdk::TurnId::from_raw(fixtures::TURN),
        inputs: Vec::new(),
        origin: bingo_sdk::TurnOrigin::Submit,
    });
    session.publish(Event::ItemCompleted {
        item: fixtures::assistant("itm_1", "The answer so far.", ItemStatus::Completed),
    });
}

#[tokio::test]
async fn a_new_session_preserves_the_previous_streamed_answer() {
    let chat = Chat::open();
    chat.say(hello("oc_1")).await;
    let old = chat.session("loopback/oc_1").await;
    streaming(&old);
    let opened = chat.records(2).await;
    let old_card = opened
        .iter()
        .find_map(|record| match record {
            Record::Send {
                id,
                mode: Mode::Stream,
                ..
            } => Some(id.clone()),
            _ => None,
        })
        .expect("the answer has a streaming card");
    chat.say(said(Conversation::direct("oc_1"), "/new", true))
        .await;
    let records = switched(&chat).await;
    let finished: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            Record::Finish { at, text } if at == &old_card => Some(text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        finished,
        ["The answer so far."],
        "switching keeps the old answer: {records:?}"
    );
    assert!(
        records.iter().any(|record| matches!(record,
            Record::Send { text, mode: Mode::Once, .. } if text == "任务已停止。"
        )),
        "the stop notice is a separate message: {records:?}"
    );

    let fresh = chat.session("loopback/oc_1").await;
    assert!(fresh.prompts().is_empty(), "the command is not a prompt");
    chat.say(said(Conversation::direct("oc_1"), "next question", true))
        .await;
    chat.until(|| (fresh.prompts() == ["next question"]).then_some(()))
        .await;
    answers(&fresh, "The new answer.").await;
    chat.until(|| {
        chat.loopback.records().into_iter().find(|record| {
            matches!(record,
                Record::Finish { at, text } if at != &old_card && text == "The new answer."
            )
        })
    })
    .await;
    assert_eq!(old.prompts(), ["run the tests"]);
}

#[tokio::test]
async fn a_new_session_preserves_the_answer_without_editing() {
    let chat = Chat::with(loopback::Config {
        edits: false,
        ..loopback::Config::default()
    });
    chat.say(hello("oc_1")).await;
    let old = chat.session("loopback/oc_1").await;
    streaming(&old);
    chat.records(1).await;
    chat.say(said(Conversation::direct("oc_1"), "/new", true))
        .await;
    let records = switched(&chat).await;
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record,
                Record::Send { text, mode: Mode::Once, .. } if text == "The answer so far."
            ))
            .count(),
        1,
        "the buffered answer is delivered exactly once: {records:?}"
    );
}

#[tokio::test]
async fn a_new_session_notice_replies_in_the_originating_thread() {
    let chat = Chat::open();
    let conversation = Conversation::group("oc_1").in_thread("omt_1");
    let parent = Posted::new("om_new");
    chat.say(Incoming::Message {
        conversation: conversation.clone(),
        principal: "ou_person".into(),
        text: "/new".into(),
        images: Vec::new(),
        addressed: true,
        parent: Some(parent.clone()),
    })
    .await;
    let records = switched(&chat).await;
    assert!(
        records.iter().any(|record| matches!(record,
            Record::Reply { to, parent: at, text, .. }
                if to == &conversation && at == &parent && text == "新会话已开启。"
        )),
        "the new-session notice retains its reply parent: {records:?}"
    );
}
