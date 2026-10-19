//! Spotify DJ ("X"): resolving its sets and the DJ's narration.
//!
//! The DJ playlist resolves to an empty shell naming a "lexicon" session; the
//! session holds the real tracks, streamed in pages (~5-track "sets"), each
//! carrying the DJ's spoken lines in its tracks' `narration.*` metadata. This
//! mirrors librespot's connect context-resolver, reimplemented here because
//! Swiftify drives the player directly instead of running spirc.

use std::collections::HashMap;

use librespot::{core::Session, protocol::context_page::ContextPage};

use crate::playlist;

/// Spotify's DJ, "X". The id is embedded in the official client's binary and is
/// the same one the librespot and go-librespot DJ work resolve.
pub const DJ_URI: &str = "spotify:playlist:37i9dQZF1EYkqdzj48dyYq";
/// the shell metadata key naming the DJ session that holds the real tracks.
const LEXICON_CONTEXT_URL: &str = "lexicon_context_url";

/// the DJ's lines for one track: its `narration.*` metadata entries.
pub(crate) type Narration = HashMap<String, String>;

/// one resolved DJ set: the tracks as web-api-shaped json (so the front end
/// reads it exactly like a playlist page), the url of the next set to fetch
/// when this one runs low, and each track's narration keyed by its uri.
pub(crate) struct DjSet {
    pub json: String,
    pub next_page_url: Option<String>,
    pub narration: HashMap<String, Narration>,
}

/// Resolves a fresh DJ set ("Let DJ pick"): follow the shell's lexicon session,
/// then ask it for a new set rather than replaying the session from the start.
pub(crate) async fn fresh_set(session: &Session) -> Result<DjSet, String> {
    let shell = session
        .spclient()
        .get_context(DJ_URI)
        .await
        .map_err(|error| format!("Spotify would not start the DJ: {error}"))?;

    // the DJ resolves to an empty shell naming the session that holds its tracks.
    let session_url = shell
        .metadata
        .get(LEXICON_CONTEXT_URL)
        .filter(|_| shell.pages.iter().all(|page| page.tracks.is_empty()))
        .cloned()
        .ok_or_else(|| {
            "Spotify did not offer a DJ session (is the DJ available on this account?)".to_owned()
        })?;

    let context = session
        .spclient()
        .get_context_url(&session_url)
        .await
        .map_err(|error| format!("Spotify would not open the DJ session: {error}"))?;

    // the session holds every set since it began, and nothing tells which were
    // heard, so a fresh start asks for a new set, as "Let DJ pick" does.
    let new_set = context
        .pages
        .last()
        .and_then(|page| page.next_page_url.clone())
        .ok_or_else(|| "the DJ session held no set".to_owned())?;
    let page = session
        .spclient()
        .get_context_page(&new_set)
        .await
        .map_err(|error| format!("Spotify would not pick a DJ set: {error}"))?;

    set_from_page(session, page).await
}

/// The next DJ set, from the `next_page_url` the previous set carried.
pub(crate) async fn more(session: &Session, next_page_url: &str) -> Result<DjSet, String> {
    let page = session
        .spclient()
        .get_context_page(next_page_url)
        .await
        .map_err(|error| format!("Spotify would not continue the DJ: {error}"))?;
    set_from_page(session, page).await
}

/// Turns a resolved set page into its tracks' json and narration.
async fn set_from_page(session: &Session, page: ContextPage) -> Result<DjSet, String> {
    let next_page_url = page.next_page_url.clone();

    let uris: Vec<String> = page
        .tracks
        .iter()
        .filter_map(|track| track.uri.clone())
        .filter(|uri| uri.starts_with("spotify:track:"))
        .collect();

    let narration = page
        .tracks
        .iter()
        .filter_map(|track| {
            let uri = track.uri.clone()?;
            let lines: Narration = track
                .metadata
                .iter()
                .filter(|(key, _)| key.starts_with("narration."))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            (!lines.is_empty()).then_some((uri, lines))
        })
        .collect();

    let tracks = playlist::track_metadata(session, &uris).await?;
    let json = playlist::items_json(&uris, &tracks)?;

    Ok(DjSet {
        json,
        next_page_url,
        narration,
    })
}
