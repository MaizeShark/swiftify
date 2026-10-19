//! a lightweight spotify connect "presence" reporter.
//!
//! swiftify plays through librespot's `Player` directly and never runs the
//! connect/spirc controller. but spotify, and so discord's "listening to
//! spotify", only reflects the *active, reporting* connect device, and the DJ
//! backend only serves its sets to a device that advertises `supports_dj`. this
//! registers swiftify as a connect device over the dealer and reports what it is
//! already playing. it never takes over playback: remote commands are only
//! acknowledged so the device stays healthy, not executed.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use librespot::{
    core::{Session, dealer::manager::Reply, dealer::protocol::Message},
    protocol::{
        connect::{
            Capabilities, Device, DeviceInfo, MemberType, PutStateReason, PutStateRequest,
        },
        devices::DeviceType,
        player::{ContextPlayerOptions, PlayOrigin, PlayerState, ProvidedTrack, Suppressions},
    },
};
use protobuf::{EnumOrUnknown, MessageField};
use tokio::sync::{oneshot, watch};

/// what the core tells the reporter about the current song. `uri` is `None`
/// whenever a spotify track is not what is playing (nothing, a local file, or a
/// stopped player), which reports the device inactive.
#[derive(Clone, Default, PartialEq)]
pub(crate) struct NowPlaying {
    pub uri: Option<String>,
    pub position_ms: u32,
    pub duration_ms: u32,
    pub is_playing: bool,
}

/// the handle the core keeps. the core feeds the current song through its own
/// `watch` sender (so playback code can report regardless of the reporter's
/// life); this handle only carries the shutdown. dropping it stops the reporter,
/// which puts the device inactive and closes the dealer on its way out.
pub(crate) struct ConnectPresence {
    _shutdown: oneshot::Sender<()>,
}

impl ConnectPresence {
    /// registers the device and begins reporting the songs that arrive on
    /// `feed`. spawns onto `runtime`, so it may be called from a thread outside
    /// the runtime's context (a synchronous ffi call).
    pub fn start(
        runtime: tokio::runtime::Handle,
        session: Session,
        device_name: String,
        feed: watch::Receiver<NowPlaying>,
    ) -> Self {
        let (shutdown, stop) = oneshot::channel();
        runtime.spawn(run(session, device_name, feed, stop));
        Self { _shutdown: shutdown }
    }
}

/// the dealer subscriptions the device needs: the pusher connection that hands
/// out the connection id each (re)connect, and the player-command channel a
/// controller sends to. the cluster subscription keeps us told of the active
/// device so we can yield when another takes over.
fn connection_id(message: Message) -> Result<String, librespot::core::Error> {
    message
        .headers
        .get("Spotify-Connection-Id")
        .cloned()
        .ok_or_else(|| librespot::core::Error::failed_precondition("no connection id in push"))
}

async fn run(
    session: Session,
    device_name: String,
    mut feed: watch::Receiver<NowPlaying>,
    mut stop: oneshot::Receiver<()>,
) {
    let mut connection_ids = match session
        .dealer()
        .listen_for("hm://pusher/v1/connections/", connection_id)
    {
        Ok(stream) => stream,
        Err(error) => {
            eprintln!("spotify-core: connect presence off, no pusher subscription: {error}");
            return;
        }
    };
    let mut commands = match session
        .dealer()
        .handle_for("hm://connect-state/v1/player/command")
    {
        Ok(stream) => stream,
        Err(error) => {
            eprintln!("spotify-core: connect presence off, no command handler: {error}");
            return;
        }
    };
    if let Err(error) = session.dealer().start().await {
        eprintln!("spotify-core: connect presence off, dealer would not start: {error}");
        return;
    }

    let mut reporter = Reporter::new(device_name);
    loop {
        tokio::select! {
            // the core dropped the presence handle: time to leave.
            _ = &mut stop => break,
            // the dealer hands out a fresh connection id on connect and on every
            // reconnect; each one re-registers the device.
            update = connection_ids.next() => match update {
                Some(Ok(id)) => {
                    session.set_connection_id(&id);
                    let now_playing = feed.borrow().clone();
                    reporter.register(&session, &now_playing).await;
                }
                Some(Err(error)) => eprintln!("spotify-core: connect id push failed: {error}"),
                None => break,
            },
            // a controller (phone, web) sends pause/next/transfer here. we do not
            // drive playback, so acknowledge success to stay a healthy device.
            command = commands.next() => match command {
                Some((_request, reply)) => { let _ = reply.send(Reply::Success); }
                None => break,
            },
            // the core reported a new song.
            changed = feed.changed() => {
                if changed.is_err() {
                    break;
                }
                let now_playing = feed.borrow().clone();
                reporter.report(&session, &now_playing).await;
            }
        }
    }

    // on the way out, leave the device inactive so controllers stop showing it.
    let _ = session.spclient().put_connect_state_inactive(true).await;
    session.dealer().close().await;
}

/// holds the little bit of state a sequence of puts needs: the device name, a
/// monotonic message id, whether the connection id has arrived, when we became
/// active, and the last state we sent (to put only on a meaningful change).
struct Reporter {
    device_name: String,
    message_id: u32,
    registered: bool,
    active_since_ms: Option<u64>,
    last_sent: Option<NowPlaying>,
}

impl Reporter {
    fn new(device_name: String) -> Self {
        Self {
            device_name,
            message_id: 1,
            registered: false,
            active_since_ms: None,
            last_sent: None,
        }
    }

    /// the first put after a connection id arrives, announcing the device.
    async fn register(&mut self, session: &Session, now_playing: &NowPlaying) {
        self.registered = true;
        self.last_sent = None;
        self.put(session, now_playing, PutStateReason::NEW_DEVICE).await;
    }

    /// a put for a song change, skipped until registered and when nothing a
    /// controller would notice has changed since the last one.
    async fn report(&mut self, session: &Session, now_playing: &NowPlaying) {
        if !self.registered || !differs(self.last_sent.as_ref(), now_playing) {
            return;
        }
        self.put(session, now_playing, PutStateReason::PLAYER_STATE_CHANGED)
            .await;
    }

    async fn put(&mut self, session: &Session, now_playing: &NowPlaying, reason: PutStateReason) {
        let now_ms = unix_millis();
        let active = now_playing.uri.is_some();
        if active && self.active_since_ms.is_none() {
            self.active_since_ms = Some(now_ms);
        } else if !active {
            self.active_since_ms = None;
        }

        let request = PutStateRequest {
            member_type: EnumOrUnknown::new(MemberType::CONNECT_STATE),
            put_state_reason: EnumOrUnknown::new(reason),
            is_active: active,
            started_playing_at: self.active_since_ms.unwrap_or(0),
            client_side_timestamp: now_ms,
            message_id: self.message_id,
            device: MessageField::some(Device {
                device_info: MessageField::some(self.device_info(session)),
                player_state: MessageField::some(player_state(session, now_playing, now_ms)),
                ..Default::default()
            }),
            ..Default::default()
        };
        self.message_id = self.message_id.wrapping_add(1);

        match session.spclient().put_connect_state_request(&request).await {
            Ok(_) => self.last_sent = Some(now_playing.clone()),
            Err(error) => eprintln!("spotify-core: connect presence put failed: {error}"),
        }
    }

    fn device_info(&self, session: &Session) -> DeviceInfo {
        DeviceInfo {
            can_play: true,
            volume: 0,
            name: self.device_name.clone(),
            device_id: session.device_id().to_string(),
            device_type: EnumOrUnknown::new(DeviceType::COMPUTER),
            device_software_version: librespot::core::version::SEMVER.to_string(),
            spirc_version: librespot::core::version::SPOTIFY_SPIRC_VERSION.to_string(),
            client_id: session.client_id(),
            capabilities: MessageField::some(Capabilities {
                volume_steps: 64,
                can_be_player: true,
                is_controllable: true,
                is_observable: true,
                gaia_eq_connect_id: true,
                needs_full_player_state: true,
                supports_transfer_command: true,
                supports_command_request: true,
                supports_playlist_v2: true,
                supports_gzip_pushes: true,
                command_acks: true,
                supported_types: vec!["audio/track".into(), "audio/episode".into()],
                // the point of running this at all beyond discord: DJ sets are
                // only served to a device that asks for them.
                supports_dj: true,
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}

/// builds the player state a controller (and discord) reads: the current track,
/// a timestamp and position it extrapolates from, and the play/pause flags.
fn player_state(session: &Session, now_playing: &NowPlaying, now_ms: u64) -> PlayerState {
    let Some(uri) = now_playing.uri.clone() else {
        return PlayerState {
            session_id: session.session_id(),
            is_system_initiated: true,
            ..Default::default()
        };
    };

    PlayerState {
        session_id: session.session_id(),
        timestamp: now_ms as i64,
        position_as_of_timestamp: i64::from(now_playing.position_ms),
        duration: i64::from(now_playing.duration_ms),
        is_playing: true,
        is_paused: !now_playing.is_playing,
        is_buffering: false,
        is_system_initiated: true,
        playback_speed: if now_playing.is_playing { 1.0 } else { 0.0 },
        context_uri: uri.clone(),
        context_url: format!("context://{uri}"),
        play_origin: MessageField::some(PlayOrigin {
            feature_identifier: "swiftify".to_string(),
            ..Default::default()
        }),
        // librespot always sends these; a controller greys out or refuses a
        // state without them.
        options: MessageField::some(ContextPlayerOptions::new()),
        suppressions: MessageField::some(Suppressions::new()),
        track: MessageField::some(ProvidedTrack {
            uri,
            provider: "context".to_string(),
            ..Default::default()
        }),
        playback_id: playback_id(now_ms),
        ..Default::default()
    }
}

/// whether a state change is worth a put: a new song, a play/pause, a changed
/// length, or a position jump larger than a normal between-put drift (a seek).
fn differs(last: Option<&NowPlaying>, now: &NowPlaying) -> bool {
    match last {
        None => true,
        Some(last) => {
            last.uri != now.uri
                || last.is_playing != now.is_playing
                || last.duration_ms != now.duration_ms
                || last.position_ms.abs_diff(now.position_ms) > 3_000
        }
    }
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_millis() as u64
}

/// a 32 hex-digit playback id. spotify expects one per playback for its stream
/// reporting; discord does not read it, so a timestamp-derived id is enough.
fn playback_id(seed: u64) -> String {
    let high = seed.rotate_left(17) ^ 0x9e37_79b9_7f4a_7c15;
    format!("{seed:016x}{high:016x}")
}
