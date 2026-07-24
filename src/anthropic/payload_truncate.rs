//! Whole-payload size guard: caps the serialized Kiro request by trimming oldest history,
//! operating on the **Anthropic request before conversion** so the converter's tool_use/tool_result
//! pairing cleanup always runs **last** and the emitted payload is guaranteed valid.
//!
//! Why before conversion (the v0.6.25 lesson): `converter::convert_request` runs three pairing
//! cleanups (orphan tool_result removal, orphan tool_use removal, non-adjacent tool_use removal)
//! that satisfy the upstream "tool_use and tool_result must be correctly paired and ordered" rule.
//! Trimming the *converted* Kiro history (as v0.6.25 did) split already-paired turns with no cleanup
//! afterward → upstream 400 "Invalid message sequence". Trimming the *Anthropic* messages and then
//! converting lets that cleanup fix any orphan a cut produced. We never have to be pairing-aware here.
//!
//! `image_resize` (per-image) and `text_truncate` (per-field) cap individual fields; this is the
//! missing **whole-payload** layer for the case where hundreds of in-budget turns add up and trip
//! AWS Q `CONTENT_LENGTH_EXCEEDS_THRESHOLD`.
//!
//! Preserved: `system` (a separate request field, never in `messages`, so untouched), the most
//! recent turns (>= [`MIN_RECENT_TURNS`]), and the current message (the last entry in `messages`,
//! always kept). A single placeholder marks where older turns were elided.
//!
//! Driven by `KIRO_RS_MAX_PAYLOAD_BYTES` (0 disables), sharing the `KIRO_RS_*` env contract.

use serde_json::Value;
use tracing::warn;

use super::converter::{ConversionError, ConversionResult, convert_request_with_mode};
use crate::model::config::ToolCompatibilityMode;
use super::types::{Message, MessagesRequest};
use crate::kiro::model::requests::kiro::KiroRequest;

/// Default whole-payload byte cap. Matches the Kiro-Go reference implementation's
/// `maxPayloadBytes = 900 * 1024` — kept conservatively below the observed upstream threshold to
/// leave room for headers and minor serialization overhead. `0` disables.
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 900 * 1024;

/// Most-recent `messages` entries always kept (current message + recent context survive).
const MIN_RECENT_TURNS: usize = 6;

/// Hard cap on trim iterations (each does one reconversion); a safety bound, normally 1–2 suffice.
const MAX_TRIM_ITERS: usize = 12;

/// Placeholder inserted (as a user turn) where older messages were dropped.
const TRUNCATION_PLACEHOLDER: &str = "[Earlier conversation history was truncated to fit the model's input limit. \
Older messages and tool activity have been omitted.]";

/// Config for whole-payload truncation. `max_bytes == 0` disables.
#[derive(Debug, Clone, Copy)]
pub struct PayloadLimitConfig {
    pub max_bytes: usize,
}

impl PayloadLimitConfig {
    /// Reads `KIRO_RS_MAX_PAYLOAD_BYTES` (0 disables), falling back to the default cap when unset.
    pub fn from_env() -> Self {
        let max_bytes = std::env::var("KIRO_RS_MAX_PAYLOAD_BYTES")
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(DEFAULT_MAX_PAYLOAD_BYTES);
        Self { max_bytes }
    }
}

/// Serialized byte size of the Kiro wire body a `ConversionResult` would produce (matches what
/// `handlers` sends and what upstream measures). Serialization failure → 0 (treated as "fits").
fn converted_payload_bytes(result: &ConversionResult) -> usize {
    let probe = KiroRequest {
        conversation_state: result.conversation_state.clone(),
        profile_arn: None,
        additional_model_request_fields: result.additional_model_request_fields.clone(),
    };
    serde_json::to_string(&probe).map(|s| s.len()).unwrap_or(0)
}

/// Serialized byte size of one Anthropic message — a cheap per-turn proxy for that turn's converted
/// contribution. Used to size the trim in a **single pass** (scaled by the observed Anthropic→Kiro
/// expansion ratio) instead of reconverting to re-measure after every drop. Failure → 0.
fn anthropic_msg_bytes(msg: &Message) -> usize {
    serde_json::to_string(msg).map(|s| s.len()).unwrap_or(0)
}

/// True if an Anthropic message is a pure tool_result turn (its `content` array holds only
/// `tool_result` blocks). Such a turn must never become the new oldest kept turn: its paired
/// `tool_use` lives in the dropped region, so the converter would strip the orphan and the turn
/// adds nothing. Dropping it too keeps the kept-window starting on a real user/assistant turn.
fn is_pure_tool_result(msg: &Message) -> bool {
    match &msg.content {
        Value::Array(arr) => {
            !arr.is_empty()
                && arr
                    .iter()
                    .all(|b| b.get("type").and_then(|v| v.as_str()) == Some("tool_result"))
        }
        _ => false,
    }
}

/// True if a message is the truncation placeholder inserted by a prior trim pass.
fn is_truncation_placeholder(msg: &Message) -> bool {
    msg.role == "user"
        && matches!(&msg.content, Value::String(s) if s == TRUNCATION_PLACEHOLDER)
}

/// Drop `drop_count` oldest messages, then keep advancing the cut until the new oldest kept turn is
/// not a pure tool_result (avoids leaving an orphan tool_result at the window head). Always keeps at
/// least [`MIN_RECENT_TURNS`] (including the current/last message). Inserts one placeholder where
/// the cut was made. Mutates `messages` in place. Returns true if anything was dropped.
fn drop_oldest_turns(messages: &mut Vec<Message>, drop_count: usize) -> bool {
    // Strip a placeholder left by a previous pass first, so it never absorbs a "drop" (otherwise a
    // re-estimating caller with drop_count==1 would drop the placeholder and re-add it — zero real
    // progress, burning iterations while staying over budget). Re-added below when we cut; if we
    // end up unable to cut, it is restored so the "context elided" note from the prior pass survives.
    let had_placeholder = messages.first().is_some_and(is_truncation_placeholder);
    if had_placeholder {
        messages.remove(0);
    }
    let restore_placeholder = |messages: &mut Vec<Message>| {
        if had_placeholder {
            messages.insert(
                0,
                Message {
                    role: "user".to_string(),
                    content: Value::String(TRUNCATION_PLACEHOLDER.to_string()),
                },
            );
        }
    };
    let n = messages.len();
    if n <= MIN_RECENT_TURNS || drop_count == 0 {
        restore_placeholder(messages);
        return false;
    }
    // Never cut into the most-recent window.
    let max_drop = n - MIN_RECENT_TURNS;
    let mut cut = drop_count.min(max_drop);
    // Advance past a leading pure-tool_result so the kept window starts clean.
    while cut < max_drop && is_pure_tool_result(&messages[cut]) {
        cut += 1;
    }
    if cut == 0 {
        restore_placeholder(messages);
        return false;
    }
    let placeholder = Message {
        role: "user".to_string(),
        content: Value::String(TRUNCATION_PLACEHOLDER.to_string()),
    };
    let tail = messages.split_off(cut);
    messages.clear();
    messages.push(placeholder);
    messages.extend(tail);
    true
}

/// Convert `payload`, trimming the **oldest Anthropic history** until the converted Kiro payload
/// fits within `cfg.max_bytes`. The converter (with its tool-pairing cleanup) runs on every attempt,
/// so the returned `ConversionResult` is always pairing-valid. No-op trimming when disabled
/// (`max_bytes == 0`) or already under budget — then it is exactly one `convert_request` call.
pub fn convert_within_limit(
    payload: &mut MessagesRequest,
    cfg: &PayloadLimitConfig,
    mode: ToolCompatibilityMode,
) -> Result<ConversionResult, ConversionError> {
    convert_within_limit_counted(payload, cfg, mode).map(|(result, _)| result)
}

/// Inner impl exposing the number of `convert_request` calls made, for the "≤ 2 conversions" test
/// guard. See [`convert_within_limit`] for behavior.
fn convert_within_limit_counted(
    payload: &mut MessagesRequest,
    cfg: &PayloadLimitConfig,
    mode: ToolCompatibilityMode,
) -> Result<(ConversionResult, usize), ConversionError> {
    let mut conversions = 1;
    let mut result = convert_request_with_mode(payload, mode)?;
    if cfg.max_bytes == 0 {
        return Ok((result, conversions));
    }
    let before = converted_payload_bytes(&result);
    if before <= cfg.max_bytes {
        return Ok((result, conversions));
    }

    // Re-estimating proportional trim (mirrors Kiro-Go's guarantee that the emitted payload fits):
    // each pass sizes the drop from per-message byte sizes scaled by the observed Anthropic→Kiro
    // expansion ratio, drops that many oldest turns, then reconverts (so the pairing cleanup runs)
    // and re-measures. Because the estimate is recomputed against the *current* overage every pass,
    // an undershoot on one pass is corrected on the next — it keeps going until the converted payload
    // is truly under `max_bytes` or nothing more can be dropped (the MIN_RECENT_TURNS floor). Uniform
    // histories converge in a single drop (≤2 conversions); uneven/tool-heavy ones take a few more.
    // MAX_TRIM_ITERS remains only as a paranoia bound against a pathological non-converging ratio.
    let mut iters = 0;
    loop {
        let current = converted_payload_bytes(&result);
        if current <= cfg.max_bytes || payload.messages.len() <= MIN_RECENT_TURNS {
            break;
        }
        if iters >= MAX_TRIM_ITERS {
            break;
        }
        // Estimate oldest turns to shed to cover the *current* converted-space overage, translated
        // back to Anthropic-space via the live expansion ratio (current converted / anthropic total).
        let msg_bytes: Vec<usize> = payload.messages.iter().map(anthropic_msg_bytes).collect();
        let anthropic_total: usize = msg_bytes.iter().sum::<usize>().max(1);
        let over_converted = current - cfg.max_bytes;
        let over_anthropic = ((over_converted as u128 * anthropic_total as u128)
            / current.max(1) as u128) as usize;
        // Walk oldest→newest accumulating message bytes until we've covered the target drop; that
        // count is how many oldest turns to shed. `drop_oldest_turns` still enforces MIN_RECENT_TURNS
        // and the pure-tool_result head guard, so an over-estimate can never cut the recent window.
        let trimmable = payload.messages.len().saturating_sub(MIN_RECENT_TURNS);
        let mut acc = 0usize;
        let mut est = 0usize;
        for &b in msg_bytes.iter().take(trimmable) {
            if acc >= over_anthropic {
                break;
            }
            acc += b;
            est += 1;
        }
        est = est.max(1);
        if !drop_oldest_turns(&mut payload.messages, est) {
            break;
        }
        result = convert_request_with_mode(payload, mode)?;
        conversions += 1;
        iters += 1;
    }

    let after = converted_payload_bytes(&result);
    warn!(
        before_bytes = before,
        after_bytes = after,
        max_bytes = cfg.max_bytes,
        remaining_messages = payload.messages.len(),
        conversions,
        "整体 payload 超字节上限，已丢弃最旧历史（单趟定位丢弃轮数，转换前裁剪，配对清理在转换时兜底）"
    );
    Ok((result, conversions))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kiro::model::requests::conversation::Message as KiroMessage;
    use serde_json::json;

    fn cfg(bytes: usize) -> PayloadLimitConfig {
        PayloadLimitConfig { max_bytes: bytes }
    }

    fn user_text(s: &str) -> Message {
        Message {
            role: "user".to_string(),
            content: json!(s),
        }
    }
    fn assistant_text(s: &str) -> Message {
        Message {
            role: "assistant".to_string(),
            content: json!(s),
        }
    }
    /// assistant turn that calls a tool (tool_use), paired with the following user tool_result.
    fn assistant_tool_use(id: &str, payload: &str) -> Message {
        Message {
            role: "assistant".to_string(),
            content: json!([
                {"type": "text", "text": "calling tool"},
                {"type": "tool_use", "id": id, "name": "do_thing", "input": {"blob": payload}}
            ]),
        }
    }
    fn user_tool_result(id: &str, payload: &str) -> Message {
        Message {
            role: "user".to_string(),
            content: json!([{"type": "tool_result", "tool_use_id": id, "content": payload}]),
        }
    }

    fn req(messages: Vec<Message>) -> MessagesRequest {
        serde_json::from_value(json!({
            "model": "claude-opus-4-8",
            "max_tokens": 4096,
            "messages": [],
        }))
        .map(|mut r: MessagesRequest| {
            r.messages = messages;
            r
        })
        .unwrap()
    }

    /// Every tool_use_id present in history assistant turns must have its tool_result in the
    /// immediately following history user turn or in the current message; no tool_result may be
    /// orphaned. Returns Err(reason) on any violation. Mirrors the upstream pairing rule.
    fn assert_pairing_valid(result: &ConversionResult) {
        let history = &result.conversation_state.history;
        // Collect tool_use ids declared by assistant turns and the ids satisfied by the very next
        // user turn (adjacency). Also collect every tool_result id that appears anywhere.
        let mut violations = Vec::new();
        for (i, m) in history.iter().enumerate() {
            if let KiroMessage::Assistant(a) = m {
                if let Some(uses) = &a.assistant_response_message.tool_uses {
                    for u in uses {
                        // the paired result must be in history[i+1] (a user turn) or current message
                        let next_has = history.get(i + 1).map(|nm| match nm {
                            KiroMessage::User(u2) => u2
                                .user_input_message
                                .user_input_message_context
                                .tool_results
                                .iter()
                                .any(|r| r.tool_use_id == u.tool_use_id),
                            _ => false,
                        });
                        let cur_has = result
                            .conversation_state
                            .current_message
                            .user_input_message
                            .user_input_message_context
                            .tool_results
                            .iter()
                            .any(|r| r.tool_use_id == u.tool_use_id);
                        if next_has != Some(true) && !cur_has {
                            violations.push(format!("orphan tool_use {}", u.tool_use_id));
                        }
                    }
                }
            }
        }
        assert!(
            violations.is_empty(),
            "pairing violations: {:?}",
            violations
        );
    }

    #[test]
    fn disabled_is_single_convert() {
        let mut p = req(vec![
            user_text("hi"),
            assistant_text("hello"),
            user_text("now"),
        ]);
        let r = convert_within_limit(&mut p, &cfg(0), ToolCompatibilityMode::ClaudeCode).unwrap();
        assert_pairing_valid(&r);
        // disabled → messages untouched (3 in, last is current → 2 history turns kept).
        assert_eq!(p.messages.len(), 3);
    }

    #[test]
    fn under_budget_untouched() {
        let mut p = req(vec![
            user_text("short"),
            assistant_text("ok"),
            user_text("q"),
        ]);
        let n = p.messages.len();
        let r = convert_within_limit(&mut p, &cfg(640_000), ToolCompatibilityMode::ClaudeCode).unwrap();
        assert_pairing_valid(&r);
        assert_eq!(p.messages.len(), n, "under budget must not trim");
    }

    #[test]
    fn over_budget_trims_and_stays_pairing_valid() {
        // Build a long tool-heavy history: many (assistant tool_use → user tool_result) pairs,
        // each big. Truncation will cut into the middle of these pairs — the converter cleanup
        // must keep the emitted payload pairing-valid (the v0.6.25 regression case).
        let mut msgs = vec![user_text("initial task")];
        for i in 0..40 {
            msgs.push(assistant_tool_use(&format!("tu_{i}"), &"a".repeat(9_000)));
            msgs.push(user_tool_result(&format!("tu_{i}"), &"b".repeat(9_000)));
        }
        msgs.push(user_text("what is the final status?")); // current message
        let mut p = req(msgs);
        let cap = 120_000;
        let r = convert_within_limit(&mut p, &cfg(cap), ToolCompatibilityMode::ClaudeCode).unwrap();
        // The whole point: no orphan tool_use/tool_result after trimming + conversion.
        assert_pairing_valid(&r);
        // And it actually shrank the payload.
        assert!(converted_payload_bytes(&r) <= cap || p.messages.len() <= MIN_RECENT_TURNS);
        // current message preserved.
        assert!(
            r.conversation_state
                .current_message
                .user_input_message
                .content
                .contains("final status")
        );
    }

    #[test]
    fn cut_landing_on_tool_result_does_not_orphan() {
        // Force a cut that would land exactly on a user tool_result (whose paired tool_use is in
        // the dropped region). is_pure_tool_result must push the window past it; after conversion
        // there must be no orphan tool_result.
        let mut msgs = vec![user_text("task")];
        for i in 0..30 {
            msgs.push(assistant_tool_use(&format!("x_{i}"), &"c".repeat(12_000)));
            msgs.push(user_tool_result(&format!("x_{i}"), &"d".repeat(12_000)));
        }
        msgs.push(user_text("done?"));
        let mut p = req(msgs);
        let r = convert_within_limit(&mut p, &cfg(100_000), ToolCompatibilityMode::ClaudeCode).unwrap();
        assert_pairing_valid(&r);
    }

    #[test]
    fn over_budget_converges_in_two_conversions() {
        // P0: uniform-ish turns → single-pass sizing lands under cap on the first drop, so exactly
        // 2 convert_request calls (initial measure + one reconvert). No 12-iteration loop.
        let mut msgs = vec![user_text("initial task")];
        for i in 0..60 {
            msgs.push(assistant_text(&format!("reply {i} {}", "a".repeat(8_000))));
            msgs.push(user_text(&format!("followup {i} {}", "b".repeat(8_000))));
        }
        msgs.push(user_text("final question"));
        let mut p = req(msgs);
        let cap = 200_000;
        let (r, conversions) = convert_within_limit_counted(&mut p, &cfg(cap), ToolCompatibilityMode::ClaudeCode).unwrap();
        assert_pairing_valid(&r);
        assert!(
            converted_payload_bytes(&r) <= cap || p.messages.len() <= MIN_RECENT_TURNS,
            "should be under cap after trim"
        );
        assert!(
            conversions <= 2,
            "single-pass sizing must converge in ≤2 conversions, got {conversions}"
        );
    }

    #[test]
    fn tool_heavy_over_budget_bounded_conversions() {
        // Even with uneven tool_use/tool_result turns, must stay well under the old 12-iter loop.
        let mut msgs = vec![user_text("task")];
        for i in 0..40 {
            msgs.push(assistant_tool_use(&format!("t_{i}"), &"a".repeat(9_000)));
            msgs.push(user_tool_result(&format!("t_{i}"), &"b".repeat(9_000)));
        }
        msgs.push(user_text("status?"));
        let mut p = req(msgs);
        let (r, conversions) = convert_within_limit_counted(&mut p, &cfg(120_000), ToolCompatibilityMode::ClaudeCode).unwrap();
        assert_pairing_valid(&r);
        assert!(
            conversions <= 4,
            "tool-heavy trim should stay bounded, got {conversions}"
        );
    }

    #[test]
    fn under_budget_single_conversion() {
        let mut p = req(vec![user_text("short"), assistant_text("ok"), user_text("q")]);
        let (_r, conversions) = convert_within_limit_counted(&mut p, &cfg(640_000), ToolCompatibilityMode::ClaudeCode).unwrap();
        assert_eq!(conversions, 1, "under budget = exactly one convert");
    }

    #[test]
    fn non_uniform_history_converges_strictly_under_cap() {
        // Regression for the 216 case: a huge oldest turn skews the single-pass estimate low, so the
        // old 1-turn-at-a-time correction (capped at 12) could stop while still over cap. The
        // re-estimating loop must drive it strictly under (plenty of trimmable turns → floor not the
        // escape). Big head + many mid turns; current message small.
        let mut msgs = vec![user_text(&"H".repeat(800_000))]; // enormous oldest turn
        for i in 0..60 {
            msgs.push(assistant_text(&format!("a{i} {}", "x".repeat(2_000))));
            msgs.push(user_text(&format!("u{i} {}", "y".repeat(2_000))));
        }
        msgs.push(user_text("final?")); // small current message
        let mut p = req(msgs);
        // Mid turns total ~240 KB — comfortably under this cap once the 800 KB head is dropped, so
        // convergence lands strictly under cap with the floor unreached (the assertions below).
        let cap = 500_000;
        let r = convert_within_limit(&mut p, &cfg(cap), ToolCompatibilityMode::ClaudeCode).unwrap();
        assert_pairing_valid(&r);
        // Floor not reached (many turns remain trimmable) → must be strictly under cap, not merely
        // "gave up at the floor".
        assert!(
            p.messages.len() > MIN_RECENT_TURNS,
            "test should leave the floor unreached to exercise convergence"
        );
        assert!(
            converted_payload_bytes(&r) <= cap,
            "re-estimating loop must land strictly under cap, got {}",
            converted_payload_bytes(&r)
        );
    }

    #[test]
    fn many_small_turns_converge_across_placeholder() {
        // Direct regression for the 216 "remaining_messages=760, 33 bytes over, conversions=13"
        // symptom: many small uniform turns where the first pass lands just barely over cap, so
        // later passes must drop *real* turns even though a placeholder now heads the list. Before
        // the fix, drop_oldest_turns(…,1) would drop+re-add the placeholder (zero progress) and burn
        // all MAX_TRIM_ITERS while staying over. It must now converge strictly under cap.
        let mut msgs = vec![user_text("start")];
        for i in 0..400 {
            msgs.push(assistant_text(&format!("a{i} {}", "x".repeat(1_500))));
            msgs.push(user_text(&format!("u{i} {}", "y".repeat(1_500))));
        }
        msgs.push(user_text("final?"));
        let mut p = req(msgs);
        let cap = 300_000;
        let (r, conversions) =
            convert_within_limit_counted(&mut p, &cfg(cap), ToolCompatibilityMode::ClaudeCode).unwrap();
        assert_pairing_valid(&r);
        assert!(
            p.messages.len() > MIN_RECENT_TURNS,
            "floor must stay unreached so convergence (not the floor) is what lands us under cap"
        );
        assert!(
            converted_payload_bytes(&r) <= cap,
            "must converge strictly under cap, got {} in {conversions} conversions",
            converted_payload_bytes(&r)
        );
        // Exactly one placeholder at the head — no accumulation across passes.
        let ph = p.messages.iter().filter(|m| is_truncation_placeholder(m)).count();
        assert_eq!(ph, 1, "exactly one truncation placeholder expected, found {ph}");
    }

    #[test]
    fn huge_single_turn_still_valid() {
        // Current message alone is enormous; cannot trim below MIN_RECENT_TURNS — must still
        // produce a pairing-valid payload (no panic, no orphan), even if over cap.
        let mut p = req(vec![
            user_text("x"),
            assistant_text("y"),
            user_text(&"z".repeat(800_000)),
        ]);
        let r = convert_within_limit(&mut p, &cfg(300_000), ToolCompatibilityMode::ClaudeCode).unwrap();
        assert_pairing_valid(&r);
    }
}
