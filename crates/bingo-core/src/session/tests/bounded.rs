use super::*;

#[tokio::test]
async fn bounded_attachment_keeps_the_atomic_cut_and_an_explicit_older_cursor() {
    let mailbox = start(ScriptedProvider::new(vec![]), vec![]);
    let huge = mailbox
        .record(ItemBody::Notice {
            level: Level::Info,
            code: "huge".into(),
            text: "x".repeat(2 * 1024 * 1024),
        })
        .await
        .unwrap();
    let recent = mailbox
        .record(ItemBody::Notice {
            level: Level::Info,
            code: "recent".into(),
            text: "tail".into(),
        })
        .await
        .unwrap();
    let (snapshot, history, mut events) = mailbox.attach_bounded(Some(512 * 1024)).await.unwrap();
    let history = history.unwrap();
    assert_eq!(snapshot.items.len(), 1);
    assert_eq!(snapshot.items[0].id, recent);
    assert_eq!(history.before, Some(recent.clone()));
    assert!(history.has_more);
    assert_eq!(history.generation, snapshot.history_generation);

    let new = mailbox
        .record(ItemBody::Notice {
            level: Level::Info,
            code: "new".into(),
            text: "after the cut".into(),
        })
        .await
        .unwrap();
    let frame = events.next().await.unwrap();
    assert_eq!(frame.seq, snapshot.seq.next());
    assert!(matches!(frame.event, Event::ItemCompleted { item } if item.id == new));

    let page = mailbox
        .history(HistoryPage {
            before: Some(recent),
            limit: 0,
            max_bytes: Some(512 * 1024),
            generation: Some(history.generation),
        })
        .await
        .unwrap();
    assert!(page.items.is_empty());
    assert_eq!(page.next, Some(huge.clone()));
    let marker = page.oversized.unwrap();
    assert_eq!(marker.id, huge);
    let pinned = mailbox
        .item_for_pin(huge, history.generation)
        .await
        .unwrap();
    let bytes = serde_json::to_vec(&pinned).unwrap();
    assert_eq!(bytes.len(), marker.total_bytes);
    assert_eq!(fnv1a64(&bytes), marker.checksum);
    assert_eq!(
        mailbox
            .item_for_pin(pinned.id.clone(), history.generation + 1)
            .await
            .unwrap_err()
            .code,
        ErrorCode::StaleGeneration
    );
}

#[tokio::test]
async fn bounded_snapshot_keeps_latest_request_after_completion_and_resume() {
    let provider = ScriptedProvider::new(vec![Script::Events(text("done"))]);
    let mailbox = start(provider, vec![]);
    let (mut full, mut events) = mailbox.attach().await.unwrap();
    mailbox.submit(IntentId::mint(), Input::text("hi", Origin::surface("test")));
    drive(&mut events, &mut full, turn_completed).await;
    assert!(full.latest_usage.is_some());
    let (bounded, _, _) = mailbox.attach_bounded(Some(512 * 1024)).await.unwrap();
    assert_eq!(bounded.latest_usage, full.latest_usage);

    let mut journal = Vec::new();
    let mut replay = mailbox.events_since(Seq::ZERO).await.unwrap();
    while let Some(Some(frame)) = replay.next().now_or_never() {
        journal.push(frame);
    }
    let resumed = resume(journal, None, Services::none(), move |_| {
        Arc::new(config(
            ScriptedProvider::new(vec![]),
            vec![],
            Arc::new(NoHost),
        ))
    })
    .unwrap();
    let (restored, _, _) = resumed.attach_bounded(Some(512 * 1024)).await.unwrap();
    assert_eq!(restored.latest_usage, full.latest_usage);
}

#[tokio::test]
async fn a_retry_dropped_cursor_is_stale_without_changing_generation() {
    let first = start(ScriptedProvider::new(vec![]), vec![]);
    first
        .record(ItemBody::Notice {
            level: Level::Info,
            code: "older".into(),
            text: "old".into(),
        })
        .await
        .unwrap();
    let removed = first
        .record(ItemBody::Notice {
            level: Level::Info,
            code: "cursor".into(),
            text: "removed".into(),
        })
        .await
        .unwrap();
    let (_, history, _) = first.attach_bounded(Some(512 * 1024)).await.unwrap();
    let generation = history.unwrap().generation;
    let mut replay = first.events_since(Seq::ZERO).await.unwrap();
    let mut journal = Vec::new();
    while let Some(Some(frame)) = replay.next().now_or_never() {
        journal.push(frame);
    }
    journal.push(Frame {
        seq: journal.last().unwrap().seq.next(),
        ts: Timestamp::now(),
        session: first.id().clone(),
        cause: None,
        event: Event::TurnRetrying {
            turn: TurnId::mint(),
            attempt: 1,
            max: 2,
            delay_ms: 0,
            dropped: vec![removed.clone()],
            reason: "retry".into(),
        },
    });
    let provider = ScriptedProvider::new(vec![]);
    let resumed = resume(journal, None, Services::none(), move |_| {
        Arc::new(config(provider, vec![], Arc::new(NoHost)))
    })
    .unwrap();
    let stale = resumed
        .history(HistoryPage {
            before: Some(removed.clone()),
            limit: 1,
            max_bytes: Some(512 * 1024),
            generation: Some(generation),
        })
        .await
        .unwrap_err();
    assert_eq!(stale.code, ErrorCode::StaleGeneration);
    let current = resumed
        .history(HistoryPage {
            before: None,
            limit: 0,
            max_bytes: Some(512 * 1024),
            generation: Some(generation),
        })
        .await
        .unwrap();
    assert_eq!(current.generation, generation);
    assert!(!current.items.iter().any(|item| item.id == removed));
}

#[tokio::test]
async fn live_only_tail_subscription_never_replays_old_journal_frames() {
    let mailbox = start(ScriptedProvider::new(vec![]), vec![]);
    mailbox
        .record(ItemBody::Notice {
            level: Level::Info,
            code: "before".into(),
            text: "old".into(),
        })
        .await
        .unwrap();
    let (cut, mut events) = mailbox.tail_events().await.unwrap();
    assert!(events.next().now_or_never().is_none());
    let id = mailbox
        .record(ItemBody::Notice {
            level: Level::Info,
            code: "after".into(),
            text: "new".into(),
        })
        .await
        .unwrap();
    let frame = events.next().await.unwrap();
    assert_eq!(frame.seq, cut.next());
    assert!(matches!(frame.event, Event::ItemCompleted { item } if item.id == id));
}
