// TODO Rename this file ingest_player_feed

use rayon::iter::ParallelIterator;
use std::fmt::{Display, Formatter};
use chrono::{DateTime, NaiveDateTime, Utc};
use hashbrown::{HashSet, HashMap};
use itertools::Itertools;
use log::{debug, error, info, warn};
use miette::IntoDiagnostic;
use mmolb_parsing::enums::{Attribute, Day};
use mmolb_parsing::feed_event::FeedEvent;
use mmolb_parsing::player_feed::ParsedPlayerFeedEventText;
use rayon::iter::IntoParallelIterator;
use serde::Deserialize;
use mmoldb_db::{db, PgConnection};
use tokio_util::sync::CancellationToken;
use chron::ChronEntity;
use mmoldb_db::models::{NewPlayerAttributeAugment, NewPlayerFeedVersion, NewPlayerParadigmShift, NewPlayerRecomposition, NewVersionIngestLog};
use mmoldb_db::taxa::Taxa;
use crate::ingest::{batch_by_entity, IngestFatalError, VersionIngestLogs};
use crate::ingest_players::day_to_db;

// I made this a constant because I'm constant-ly terrified of typoing
// it and introducing a difficult-to-find bug
const PLAYER_FEED_KIND: &'static str = "player_feed";
const CHRON_FETCH_PAGE_SIZE: usize = 1000;
const RAW_PLAYER_FEED_INSERT_BATCH_SIZE: usize = 1000;
const PROCESS_PLAYER_FEED_BATCH_SIZE: usize = 1000;

pub async fn ingest_player_feeds(ingest_id: i64, pg_url: String, abort: CancellationToken) -> miette::Result<()> {
    crate::ingest::ingest(
        ingest_id,
        PLAYER_FEED_KIND,
        CHRON_FETCH_PAGE_SIZE,
        RAW_PLAYER_FEED_INSERT_BATCH_SIZE,
        PROCESS_PLAYER_FEED_BATCH_SIZE,
        pg_url,
        abort,
        db::get_player_feed_ingest_start_cursor,
        ingest_page_of_player_feeds,
    ).await
}

pub fn ingest_page_of_player_feeds(
    taxa: &Taxa,
    raw_player_feeds: Vec<ChronEntity<serde_json::Value>>,
    conn: &mut PgConnection,
    worker_id: usize,
) -> miette::Result<i32> {
    debug!(
        "Starting of {} player feeds on worker {worker_id}",
        raw_player_feeds.len()
    );
    let save_start = Utc::now();

    #[derive(Deserialize)]
    struct FeedContainer {
        feed: Vec<FeedEvent>,
    }

    let deserialize_start = Utc::now();
    // TODO Gracefully handle player feed deserialize failure
    let player_feeds = raw_player_feeds
        .into_par_iter()
        .map(|game_json| {
            Ok::<ChronEntity<FeedContainer>, serde_json::Error>(ChronEntity {
                kind: game_json.kind,
                entity_id: game_json.entity_id,
                valid_from: game_json.valid_from,
                valid_to: game_json.valid_to,
                data: serde_json::from_value(game_json.data)?,
            })
        })
        .collect::<Result<Vec<_>, _>>().into_diagnostic()?;
    let deserialize_duration = (Utc::now() - deserialize_start).as_seconds_f64();
    debug!(
        "Deserialized page of {} player feeds in {:.2} seconds on worker {}",
        player_feeds.len(), deserialize_duration, worker_id
    );

    let latest_time = player_feeds
        .last()
        .map(|version| version.valid_from)
        .unwrap_or(Utc::now());
    let time_ago = latest_time.signed_duration_since(Utc::now());
    let human_time_ago = chrono_humanize::HumanTime::from(time_ago);

    // Convert to Insertable type
    let new_player_feeds = player_feeds
        .iter()
        .map(|v| chron_player_feed_as_new(&taxa, &v.entity_id, v.valid_from, &v.data.feed, None))
        .collect_vec();

    // The handling of valid_until is entirely in the database layer, but its logic
    // requires that a given batch of players does not have the same player twice. We
    // provide that guarantee here.
    let new_player_feeds_len = new_player_feeds.len();
    let mut total_inserted = 0;
    for batch in batch_by_entity(new_player_feeds, |v| v.0.mmolb_player_id) {
        let to_insert = batch.len();
        info!(
            "Sent {} new player feed versions out of {} to the database.",
            to_insert,
            new_player_feeds_len,
        );

        let inserted = db::insert_player_feed_versions(conn, batch).into_diagnostic()?;
        total_inserted += inserted as i32;

        info!(
            "Sent {} new player feed versions out of {} to the database. \
            {inserted} versions were actually inserted, the rest were duplicates. \
            Currently processing player feeds from {human_time_ago}.",
            to_insert,
            player_feeds.len(),
        );
    }

    let save_duration = (Utc::now() - save_start).as_seconds_f64();

    info!(
        "Ingested page of {} player feeds in {save_duration:.3} seconds.",
        player_feeds.len(),
    );

    Ok(total_inserted)
}


fn process_paradigm_shift<'e>(
    changing_attribute: Attribute,
    value_attribute: Attribute,
    paradigm_shifts: &mut Vec<NewPlayerParadigmShift<'e>>,
    feed_event_index: i32,
    event: &FeedEvent,
    time: NaiveDateTime,
    player_id: &'e str,
    taxa: &Taxa,
) {
    if changing_attribute == Attribute::Priority {
        let (day_type, day, superstar_day) = day_to_db(&event.day, taxa);

        paradigm_shifts.push(NewPlayerParadigmShift {
            mmolb_player_id: player_id,
            feed_event_index,
            time,
            season: event.season as i32,
            day_type,
            day,
            superstar_day,
            attribute: taxa.attribute_id(value_attribute.into()),
        })
    } else {
        // TODO Expose player ingest errors on the site
        error!(
            "Encountered an AttributeEquals feed event that changes an \
            attribute other than priority. Player {} feed event {} changes {}",
            player_id, feed_event_index, changing_attribute,
        );
    }
}

pub fn chron_player_feed_as_new<'a>(
    taxa: &Taxa,
    player_id: &'a str,
    valid_from: DateTime<Utc>,
    feed_items: &'a [FeedEvent],
    mut final_player_name: Option<&str>,
) -> (
    NewPlayerFeedVersion<'a>,
    Vec<NewPlayerAttributeAugment<'a>>,
    Vec<NewPlayerParadigmShift<'a>>,
    Vec<NewPlayerRecomposition<'a>>,
    Vec<NewVersionIngestLog<'a>>,
) {
    let mut ingest_logs = VersionIngestLogs::new(PLAYER_FEED_KIND, player_id, valid_from);

    // TODO make this static, or at least global
    // Some feed events were accidentally reverted (by caching issues I think), and we want to
    // pretend they never happened
    let impermanent_feed_events = {
        let mut hashes = HashSet::new();
        #[rustfmt::skip]
        hashes.extend(
            [
                // format: (player id, timestamp of feed event). Not timestamp of player update.
                ("6805db0cac48194de3cd40dd", "2025-07-14T12:32:16.183651+00:00"),
                ("6840fa75ed58166c1895a7f3", "2025-07-14T12:58:25.172157+00:00"),
                ("6840fb13e63d9bb8728896d2", "2025-07-14T11:56:08.156319+00:00"),
                ("6840fe6508b7fc5e21e8a940", "2025-07-14T15:04:09.335705+00:00"),
                ("6841000988056169e0078792", "2025-07-14T11:58:34.656639+00:00"),
                ("684102aaf7b5d3bf791d67e8", "2025-07-14T12:54:23.274555+00:00"),
                ("684102dfec9dc637cfd0cad6", "2025-07-14T12:01:51.299731+00:00"),
                ("68412d1eed58166c1895ae66", "2025-07-14T12:20:36.597014+00:00"),
                ("68418c52554d8039701f1c93", "2025-07-14T12:09:42.764129+00:00"),
                ("6846cc4d4a488309816674ff", "2025-07-14T12:10:19.571083+00:00"),
                ("684727f5bb00de6f9bb79973", "2025-07-14T12:14:14.710296+00:00"),
                ("68505bc5341f7f2421020d05", "2025-07-14T12:13:58.129103+00:00"),
                ("6855b350f1d8f657407b231c", "2025-07-14T15:02:55.607448+00:00"),
                ("68564374acfab5652c3a6c44", "2025-07-15T01:02:41.992667+00:00"),
                ("685b740338c6569da104aa48", "2025-07-14T12:47:59.895679+00:00"),
                ("686355f4b254dfbaab3014b0", "2025-07-14T11:51:34.711389+00:00"),
                ("68655942f27aa83a88fa64e0", "2025-07-14T10:36:47.308576+00:00"),
                ("6840fb13e63d9bb8728896d2", "2025-07-14T11:56:08.156319+00:00"),
            ]
            .into_iter()
            .map(|(id, dt)| (id, DateTime::parse_from_rfc3339(dt).unwrap().naive_utc()))
        );
        hashes
    };

    // TODO make this static, or at least global
    // Some apparent recompositions happened without a feed event
    let mut inferred_recompositions = {
        let mut hashes = HashMap::new();
        #[rustfmt::skip]
        hashes.extend(
            [
                // format: (player id, [list of (earliest timestamp associated with new name, (season, day, old name, new name, ))])
                ("68464b8e144e874e6deb8d31", [
                    ("2025-07-15T04:50:01.666353Z", (3, Day::Day(19), "Avery Quiñones", "Ingrid Fontana"))
                ]),
                ("68421fe0f415a392f7340229", [
                    ("2025-07-15T05:01:10.638951Z", (3, Day::Day(19), "December Brown", "Hector Strawberry"))
                ]),
                ("6841b52de63d9bb87288a03f", [
                    ("2025-07-15T05:41:04.705262Z", (3, Day::Day(20), "Colton Kılıç", "Ahmed Yamauchi"))
                ]),
                ("6840fe6508b7fc5e21e8a940", [
                    ("2025-07-15T05:49:14.117891Z", (3, Day::Day(20), "Lauren Werleman", "Melanie Sasaki"))
                ]),
                ("68410547295b2368c0ac7617", [
                    ("2025-07-15T05:51:28.998923Z", (3, Day::Day(20), "Maybelline Correia", "Natasha Mortensen"))
                ]),
                ("6844d4e3925dd4f9d72ac6f8", [
                    ("2025-07-15T05:51:52.377638Z", (3, Day::Day(20), "Lifen Davies", "Test Berglund"))
                ]),
                ("684114238ba62cf2ce8d9138", [
                    ("2025-07-15T05:52:52.675357Z", (3, Day::Day(20), "Gabriela Gentry", "Leo Minto"))
                ]),
                ("6845e23cf7b5d3bf791d6fb3", [
                    ("2025-07-15T05:56:56.821444Z", (3, Day::Day(20), "Sienna McIntosh", "Finn Anderson"))
                ]),
                ("6840ff3b88056169e0078756", [
                    ("2025-07-15T05:57:27.079243Z", (3, Day::Day(20), "Tina Jamal", "Tina Suleiman"))
                ]),
                ("6840fb6a183c892d88a0fa6c", [
                    ("2025-07-15T05:57:47.284153Z", (3, Day::Day(20), "Qingyu Figueredo", "Frances Radcliffe"))
                ]),
                ("684308b088056169e0078efc", [
                    ("2025-07-15T05:59:51.822743Z", (3, Day::Day(20), "Arya Garcia", "Katherine Cortes"))
                ]),
                ("684df75227a5c3fdfbb7c316", [
                    ("2025-07-15T05:59:52.045565Z", (3, Day::Day(20), "Randy Quiros", "Sherry Huff"))
                ]),
                ("684324d3f415a392f734030d", [
                    ("2025-07-15T06:06:41.942801Z", (3, Day::Day(21), "Randall McDaniel", "Geo Dale"))
                ]),
                ("6840ff75554d8039701f16bb", [
                    ("2025-07-15T06:06:44.704504Z", (3, Day::Day(21), "Ray O'Keeffe", "Piper Norman"))
                ]),
                ("684104ba144e874e6deb8486", [
                    ("2025-07-15T06:06:45.091796Z", (3, Day::Day(21), "McBaseball Korhonen", "Mattie Quesada"))
                ]),
                ("6840faf3554d8039701f1403", [
                    ("2025-07-15T06:06:59.526678Z", (3, Day::Day(21), "Elmer Pinheiro", "Nellie Smith"))
                ]),
                ("68410083ed58166c1895ab31", [
                    ("2025-07-15T06:08:04.527243Z", (3, Day::Day(21), "Terri de Araujo", "Faith Ellington"))
                ]),
                ("68411461ed58166c1895ad86", [
                    ("2025-07-15T06:08:19.11844Z", (3, Day::Day(21), "Kristen Stephenson", "Clayton MCIntyre"))
                ]),
                ("68410538ec9dc637cfd0cb65", [
                    ("2025-07-15T06:08:22.463221Z", (3, Day::Day(21), "Tracey Lajoie", "Harper Buckley"))
                ]),
                ("6843a73a554d8039701f1fcd", [
                    ("2025-07-15T06:08:40.965006Z", (3, Day::Day(21), "Aurora Wakefield", "Martin Bertrand"))
                ]),
                ("6840ff81144e874e6deb8366", [
                    ("2025-07-15T06:08:42.251327Z", (3, Day::Day(21), "Margaret Van", "Luther McCarthy"))
                ]),
                ("6840ff26925dd4f9d72abd89", [
                    ("2025-07-15T06:08:44.979727Z", (3, Day::Day(21), "Roberta Johnston", "Test O'Grady"))
                ]),
                ("684101fded58166c1895ab91", [
                    ("2025-07-15T06:17:02.400941Z", (3, Day::Day(21), "Claude Morozova", "Kilroy Belanger"))
                ]),
                ("68410658144e874e6deb84e9", [
                    ("2025-07-15T06:17:10.624954Z", (3, Day::Day(21), "D. Umaru", "Travis Bashir"))
                ]),
                ("684102dfec9dc637cfd0cad6", [
                    ("2025-07-15T06:17:28.508744Z", (3, Day::Day(21), "Jeff Stojanovska", "Fizzbuzz Grigoryan"))
                ]),
                ("684149eb8d67c531e89fe3b0", [
                    ("2025-07-15T06:17:54.144151Z", (3, Day::Day(21), "Sophia Caldwell", "Wendy Akhtar"))
                ]),
                ("684107b054a7fbd4133871ed", [
                    ("2025-07-15T06:18:24.926365Z", (3, Day::Day(21), "Bessie Jovanović", "Verônica Olyinyk"))
                ]),
                ("6841d38d925dd4f9d72ac46a", [
                    ("2025-07-15T06:18:28.014537Z", (3, Day::Day(21), "Jason Rakotomalala", "Andrea Colon"))
                ]),
                ("6840fcb5f415a392f733f93e", [
                    ("2025-07-15T06:18:59.011472Z", (3, Day::Day(21), "Diego Wright", "Novak Hunt"))
                ]),
                ("6842876af7b5d3bf791d6db9", [
                    ("2025-07-15T06:18:59.011472Z", (3, Day::Day(21), "Zerobbabel Perera", "Bear Lindberg"))
                ]),
                ("684cd753b828819cba964640", [
                    ("2025-07-15T06:19:09.501165Z", (3, Day::Day(21), "Todd Becker", "Miles Mooketsi"))
                ]),
                ("68411e11183c892d88a0ff7b", [
                    ("2025-07-15T06:19:10.135468Z", (3, Day::Day(21), "Addie Frederiksen", "Jane Werner"))
                ]),
                ("684108f8e2d7557e153cc5c0", [
                    ("2025-07-15T06:27:14.671061Z", (3, Day::Day(21), "Freddie Acevedo", "Ketlen Crowley"))
                ]),
                ("68475f9cbb00de6f9bb799ee", [
                    ("2025-07-15T06:29:36.052261Z", (3, Day::Day(21), "Darryl Segura", "Nataniela Tejada"))
                ]),
                ("6866b2b402f61969e2ced8b5", [
                    ("2025-07-15T06:30:03.321192Z", (3, Day::Day(21), "Tanya Morita", "Paul Abreu"))
                ]),
                ("68418ac7144e874e6deb8860", [
                    ("2025-07-15T06:38:48.788294Z", (3, Day::Day(21), "Rex Köse", "Dora Wiggins"))
                ]),
                ("684148178ba62cf2ce8d92f6", [
                    ("2025-07-15T06:39:09.516311Z", (3, Day::Day(21), "Sophia Chavarria", "Marcelle Thornton"))
                ]),
                ("68655d8277149281c81ea917", [
                    ("2025-07-18T05:04:14.432935Z", (3, Day::Day(92), "Lucy Haque", "Yara Sin"))
                ]),
                ("684cec93523adf827ba420e4", [
                    ("2025-07-22T05:08:47.992513Z", (3, Day::Day(170), "Algernon Larsson", "Dustin Randybass"))
                ]),
                ("684102af295b2368c0ac759d", [
                    ("2025-07-22T05:09:34.679498Z", (3, Day::Day(170), "Cathy Schröder", "Sandra Sakamoto"))
                ]),
                ("684128b38ba62cf2ce8d91f8", [
                    ("2025-07-23T05:09:54.563711Z", (3, Day::Day(194), "Harmon Matsui", "Myra Tomlinson"))
                ]),
                ("6879d391762fe1c1e4769883", [
                    ("2025-07-26T00:42:41.224451Z", (3, Day::PostseasonPreview, "Han-Soo van der Meer", "Tommy van den Berg"))
                ]),
                ("6840feff8ba62cf2ce8d8e41", [
                    ("2025-07-25T18:14:08.672137Z", (3, Day::PostseasonPreview, "Asher Boyd", "Algernon Welch"))
                ]),
                ("684339118ba62cf2ce8d9620", [
                    ("2025-07-26T09:50:09.471896Z", (3, Day::PostseasonPreview, "Dilly-Dally Sarkar", "July Shah"))
                ]),
                ("687e44b6a18a39dc5d280cb8", [
                    ("2025-07-26T06:06:08.474301Z", (3, Day::PostseasonPreview, "Hobble Schulz", "Chorby Schneider"))
                ]),
                ("687c4009af79e504eb851455", [
                    ("2025-07-25T21:24:49.851079Z", (3, Day::PostseasonPreview, "Neptune Grgić", "Boyfriend Otsuka"))
                ]),
                ("68412307896f631e9d6892c8", [
                    ("2025-07-26T01:33:02.907220Z", (3, Day::PostseasonPreview, "Ahmed Nunes", "Francisco Langston"))
                ]),
                ("6847d0c44a488309816676d6", [
                    ("2025-07-25T22:21:19.095142Z", (3, Day::PostseasonPreview, "Dawn Injury", "Georgia Dracula"))
                ]),
                ("684102fa183c892d88a0fd3a", [
                    ("2025-07-25T20:05:23.830022Z", (3, Day::PostseasonPreview, "Esther Carlington", "Ally Mashaba"))
                ]),
                ("68751f7fc1f9dc22d3a8f267", [
                    ("2025-07-26T00:44:53.626041Z", (3, Day::PostseasonPreview, "Hitchcock Whitaker", "Mamie Ling"))
                ]),
                ("6875b958fa3eb3ff9c319ec4", [
                    ("2025-07-25T19:43:36.410787Z", (3, Day::PostseasonPreview, "Pogo-Stick Curran", "Maybelline Lehtinen"))
                ]),
                ("68410cb088056169e0078998", [
                    ("2025-07-26T05:03:28.822305Z", (3, Day::PostseasonPreview, "Chloe Alfonso", "Fannie Oconnor"))
                ]),
            ]
                .into_iter()
                .map(|(id, recompositions)| {
                    (
                        id, recompositions.map(|(dt, names)| {
                            (DateTime::parse_from_rfc3339(dt).unwrap().naive_utc(), names)
                        })
                    )
                })
        );
        hashes
    };
    // TODO make this static, or at least global
    // Some recompositions were overwritten by a subsequent augment:
    // https://discord.com/channels/1136709081319604324/1401504610732216470
    let overwritten_recompositions = {
        let mut hashes = HashMap::new();
        #[rustfmt::skip]
        hashes.extend(
            [
                // format: ((id, feed event index), (old name, new name, recompose time, season, day)
                (("68414353f7b5d3bf791d6af7", 14), ("Fernando MacDonald", "Drummer Powell", "2025-07-24T04:05:51.465692+00:00", 3, Day::Day(217))),
                (("6850da687db123b15516c542", 12), ("Josie Frandsen", "Sophia Wisp", "2025-07-21T04:10:59.130028+00:00", 3, Day::Day(145))),
                (("684104c708b7fc5e21e8ab83", 15), ("K. Miyazaki", "Will Thome", "2025-07-22T04:07:16.339620+00:00", 3, Day::Day(169))),
                (("68413fd1183c892d88a10074", 22), ("King Sapphire", "Ian Fisher", "2025-07-19T04:47:47.912546+00:00", 3, Day::Day(115))),
                (("6845d8ba88056169e0079081", 14), ("Lucy O'Toole", "Darren Prasad", "2025-07-19T04:41:04.044671+00:00", 3, Day::Day(115))),
                (("68751e24d9c3888e3e26a328", 1), ("Myra Elemefayo", "Nibbler Grigoryan", "2025-07-18T04:29:33.564533+00:00", 3, Day::Day(91))),
                (("6841030dec9dc637cfd0cade", 16), ("Noodlehead Li", "Muffin Major", "2025-07-16T04:32:17.924992+00:00", 3, Day::Day(43))),
                (("6840fcd8ed58166c1895a9a5", 14), ("Novak Mann", "Quackers Hunter", "2025-07-21T03:57:06.092120+00:00", 3, Day::Day(144))),
                (("6840fa88896f631e9d688d28", 18), ("Popquiz Beltre", "Jill Pratt", "2025-07-22T04:44:04.614065+00:00", 3, Day::Day(169))),
                (("684674e9ec9dc637cfd0d407", 11), ("Seok-Jin Clarke", "Jupiter Lucas", "2025-07-23T04:24:10.714881+00:00", 3, Day::Day(193))),
                (("6840faf7e63d9bb8728896c0", 12), ("Test Estrada", "Mateo Kawashima", "2025-07-16T04:01:27.045974+00:00", 3, Day::Day(43))),
                (("6805db0cac48194de3cd405f", 6), ("Waylon Osman", "Ziggy Lysenko", "2025-07-27T21:17:26.893992+00:00", 3, Day::Holiday)),
                (("68751f7fc1f9dc22d3a8f267", 7), ("Yasmin Barlow", "Hudson Berg", "2025-07-22T04:51:04.832373+00:00", 3, Day::Day(169))),
            ]
            .into_iter()
            .map(|(key, (nb, na, dt, s, d))| (key, (nb, na, DateTime::parse_from_rfc3339(dt).unwrap().naive_utc(), s, d)))
        );
        hashes
    };

    fn name_matches(a: &str, b: &str) -> bool {
        // Multiple Stanleys Demir were generated during the s2
        // Falling Stars event and then Danny manually renamed them
        a == b || (a.starts_with("Stanley Demir") && b.starts_with("Stanley Demir"))
    }

    // Option<expected name, Option<temporarily allowed name>>
    //    currently "temporarily allowed name" is always allowed
    //    for the rest of this exact feed event only
    struct CheckPlayerName<'a> {
        known_name: Option<&'a str>,
        temporary_name_override: Option<&'a str>,
    }
    impl<'a> CheckPlayerName<'a> {
        pub fn new() -> Self {
            Self { known_name: None, temporary_name_override: None }
        }

        pub fn check_or_set_name(&mut self, name: &'a str, ingest_logs: &mut VersionIngestLogs) {
            if let Some(n) = self.temporary_name_override {
                if !name_matches(name, n) {
                    // This is just for a better error message
                    if let Some(n2) = self.known_name {
                        if name_matches(name, n2) {
                            ingest_logs.error(format!(
                                "Player name from feed event (\"{name}\") does not match the \
                                current temporary name override (\"{n}\"). Note: It does match \
                                the current non-override name (\"{n2}\")."
                            ));
                        } else {
                            ingest_logs.error(format!(
                                "Player name from feed event (\"{name}\") does not match the \
                                current temporary name override (\"{n}\"). Note: It also does \
                                not match the current non-override name (\"{n2}\")."
                            ));
                        }
                    } else {
                        ingest_logs.error(format!(
                            "Player name from feed event (\"{name}\") does not match the \
                            current temporary name override (\"{n}\"). Note: The current \
                            non-override name is not known."
                        ));
                    }
                }
            } else if let Some(n) = self.known_name {
                if !name_matches(name, n) {
                    ingest_logs.error(format!(
                        "Player name from feed event (\"{name}\") does not match the known \
                        player name (\"{n}\")."
                    ));
                }
            } else {
                self.known_name = Some(name);
            }
        }

        pub fn set_known_name(&mut self, new_name: &'a str) {
            self.known_name = Some(new_name);
        }

        pub fn set_temporary_name_override(&mut self, new_name_override: &'a str) {
            self.temporary_name_override = Some(new_name_override);
        }

        pub fn clear_temporary_name_override(&mut self) {
            self.temporary_name_override = None;
        }
    }

    impl Display for CheckPlayerName<'_> {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            match self.known_name {
                None => { write!(f, "unknown player name") }
                Some(name) => { write!(f, "'{name}'") }
            }
        }
    }

    // Note that the name at the start of the feed is not (necessarily) final_player_name
    let mut check_player_name = CheckPlayerName::new();

    let player_feed_version = NewPlayerFeedVersion {
        mmolb_player_id: player_id,
        valid_from: valid_from.naive_utc(),
        valid_until: None,
        num_entries: feed_items.len() as i32,
    };

    // Current plan is to regenerate all feed-dependent tables every
    // time a player is ingested, and use a database function to handle
    // the many duplicates that will create.
    let mut attribute_augments = Vec::new();
    let mut paradigm_shifts = Vec::new();
    let mut recompositions = Vec::new();

    let mut pending_inferred_recompositions = inferred_recompositions.get_mut(player_id).map(|r| r.into_iter().peekable());

    // This will *almost* always be equal to feed_items.len(), but not
    // when there is an impermanent event
    let mut max_permanent_feed_event_index_plus_one = 0;
    for (index, event) in feed_items.iter().enumerate() {
        let feed_event_index = index as i32;
        let time = event.timestamp.naive_utc();

        check_player_name.clear_temporary_name_override();

        let mut inferred_event_index = 0;
        if impermanent_feed_events.contains(&(player_id, time)) {
            // TODO Add this as a pair of recompose/unrecompose events, so the attributes
            //   in between are accurate
            ingest_logs.info(format!(
                "Skipping feed event \"{}\" because it was reverted later",
                event.text,
            ));
            if index + 1 != feed_items.len() {
                ingest_logs.warn(format!(
                    "This non-permanent event is not the last event in the feed ({} of {}).",
                    index + 1,
                    feed_items.len(),
                ));
            }

            // Impermanent feed events still change the player name, but since we
            // skip them we don't catch the change and the check at the end of
            // the feed processing will fail. Pretend the name is unknown to
            // skip the check.
            final_player_name = None;
            continue;
        }

        max_permanent_feed_event_index_plus_one = feed_event_index + 1;

        // Apply any inferred recompositions whose time is before this event's time
        if let Some(pending) = &mut pending_inferred_recompositions {
            while let Some((time, info)) = pending.next_if(|(dt, _)| *dt <= time) {
                let (season, day, player_name_before, player_name_after) = info;
                ingest_logs.info(format!(
                    "Applying inferred recomposition from {} to {}",
                    player_name_before, player_name_after
                ));
                check_player_name.check_or_set_name(player_name_before, &mut ingest_logs);
                check_player_name.set_known_name(player_name_after);
                let (day_type, day, superstar_day) = day_to_db(&Ok(*day), taxa);
                recompositions.push(NewPlayerRecomposition {
                    mmolb_player_id: player_id,
                    feed_event_index,
                    inferred_event_index: Some(inferred_event_index),
                    time: *time,
                    season: *season,
                    day_type,
                    day,
                    superstar_day,
                    player_name_before,
                    player_name_after,
                    reverts_recomposition: None,
                });
                inferred_event_index += 1;
            }
        };

        let parsed_event = mmolb_parsing::player_feed::parse_player_feed_event(event);
        let skip_this_event = if let Some(info) = overwritten_recompositions.get(&(player_id, feed_event_index)) {
            let (player_name_before, player_name_after, recompose_time, season, day) = info;

            check_player_name.check_or_set_name(player_name_before, &mut ingest_logs);
            // There shouldn't be any subsequent events, but there is still
            // the player_final_name check so we do have to update the name
            check_player_name.set_known_name(player_name_after);
            ingest_logs.debug(format!("Set expected player name to {player_name_after}"));

            ingest_logs.debug(format!(
                "Inserting implied Recomposed event for overwritten Recompose from \
                {player_name_before} to {player_name_after}",
            ));
            let (day_type, day, superstar_day) = day_to_db(&Ok(*day), taxa);
            recompositions.push(NewPlayerRecomposition {
                mmolb_player_id: player_id,
                feed_event_index,
                inferred_event_index: Some(inferred_event_index),
                time: *recompose_time,
                season: *season,
                day_type,
                day,
                superstar_day,
                player_name_before,
                player_name_after,
                reverts_recomposition: None,
            });
            inferred_event_index += 1;

            if let ParsedPlayerFeedEventText::Recomposed { new, previous } = &parsed_event {
                // The first time we hit one of these, it'll be a Recompose event
                ingest_logs.debug(format!(
                    "Checking and skipping overwritten Recomposed event from {previous} to {new}",
                ));
                // This is a real event now, but it's marked as overwritten,
                // which means it's about to be deleted and become implied.
                // So we ignore the real version and insert the inferred version
                if new != player_name_after {
                    ingest_logs.error(format!(
                        "The overwritten Recomposed event new player name didn't match: expected \
                        {player_name_after}, but observed {new}.",
                    ));
                }
                if previous != player_name_before {
                    ingest_logs.error(format!(
                        "The overwritten Recomposed event previous player name didn't match: \
                        expected {player_name_before}, but observed {previous}.",
                    ));
                }
                if *recompose_time != time {
                    ingest_logs.error(format!(
                        "The overwritten Recomposed event timestamp didn't match: \
                        expected {recompose_time}, but observed {}.",
                        event.timestamp.naive_utc(),
                    ));
                }
                true
            } else {
                ingest_logs.debug(format!(
                    "Inserting unrecompose event to reset {player_name_after}'s attributes",
                ));
                // If the event in question is *not* a recompose event, that means this is the
                // event that reverted the recompose. We need to insert the unrecompose event.
                recompositions.push(NewPlayerRecomposition {
                    mmolb_player_id: player_id,
                    feed_event_index,
                    inferred_event_index: Some(inferred_event_index),
                    // This very event is our first observation of the unrecompose
                    time,
                    // Due to the nature of this particular bug, the seasonday is the same for the
                    // original recompose and the unrecompose.
                    season: *season,
                    day_type,
                    day,
                    superstar_day,
                    // Player name doesn't change
                    player_name_before: player_name_after,
                    player_name_after,
                    reverts_recomposition: Some(*recompose_time),
                });
                // I want to make sure inferred_event_index is correct if another use of it is added
                #[allow(unused_assignments)]
                { inferred_event_index += 1 };

                // The long-term name will be player_name_after...
                check_player_name.set_known_name(player_name_after);
                // ...but for this feed event, it will be player_name_before
                check_player_name.set_temporary_name_override(player_name_before);

                false
            }
        } else {
            false
        };

        if skip_this_event {
            ingest_logs.debug(format!(
                "Skipping event \"{}\" because `skip_this_event` told us to", event.text,
            ));
            continue;
        }

        match parsed_event {
            ParsedPlayerFeedEventText::ParseError { error, text } => {
                // TODO Expose player ingest errors on the site
                ingest_logs.error(format!(
                    "Error {error} parsing {text} from {} ({})'s feed",
                    check_player_name, player_id,
                ));
            }
            ParsedPlayerFeedEventText::Delivery { .. } => {
                // We don't (yet) use this event, but feed events have a timestamp so it
                // could be used to backdate when players got their item. Although it
                // doesn't really matter because player items can't (yet) change during
                // a game, so we can backdate any player items to the beginning of any
                // game they were observed doing. Also this doesn't apply to item changes
                // that team owners make using the inventory, so there's not much point.
            }
            ParsedPlayerFeedEventText::Shipment { .. } => {
                // See comment on Delivery
            }
            ParsedPlayerFeedEventText::SpecialDelivery { .. } => {
                // See comment on Delivery
            }
            ParsedPlayerFeedEventText::AttributeChanges { player_name, attribute, amount } => {
                let (day_type, day, superstar_day) = day_to_db(&event.day, taxa);

                check_player_name.check_or_set_name(player_name, &mut ingest_logs);
                attribute_augments.push(NewPlayerAttributeAugment {
                    mmolb_player_id: player_id,
                    feed_event_index,
                    time,
                    season: event.season as i32,
                    day_type,
                    day,
                    superstar_day,
                    attribute: taxa.attribute_id(attribute.into()),
                    value: amount as i32,
                })
            }
            ParsedPlayerFeedEventText::AttributeEquals {
                player_name,
                changing_attribute,
                value_attribute,
            } => {
                check_player_name.check_or_set_name(&player_name, &mut ingest_logs);
                // The handling of a non-priority SingleAttributeEquals will have to
                // be so different that it's not worth trying to implement before it
                // actually appears
                process_paradigm_shift(
                    changing_attribute,
                    value_attribute,
                    &mut paradigm_shifts,
                    feed_event_index,
                    event,
                    time,
                    player_id,
                    taxa,
                )
            }
            ParsedPlayerFeedEventText::TakeTheMound { .. } => {
                // We can use this to backdate certain position changes, but
                // the utility of doing that is limited for the same reason
                // as the utility of processing Delivery is limited.
            }
            ParsedPlayerFeedEventText::TakeThePlate { .. } => {
                // See comment on TakeTheMound
            }
            ParsedPlayerFeedEventText::SwapPlaces { .. } => {
                // See comment on TakeTheMound
            }
            ParsedPlayerFeedEventText::Enchantment { .. } => {
                // See comment on Delivery
            }
            ParsedPlayerFeedEventText::InjuredByFallingStar { .. } => {
                // We don't (yet) have a use for this event
            }
            ParsedPlayerFeedEventText::InfusedByFallingStar { .. } => {
                // We can use this to backdate certain mod changes, but
                // the utility of doing that is limited for the same reason
                // as the utility of processing Delivery is limited.
            }
            ParsedPlayerFeedEventText::Recomposed { new, previous } => {
                let (day_type, day, superstar_day) = day_to_db(&event.day, taxa);

                check_player_name.check_or_set_name(previous, &mut ingest_logs);
                check_player_name.set_known_name(new);
                recompositions.push(NewPlayerRecomposition {
                    mmolb_player_id: player_id,
                    feed_event_index,
                    inferred_event_index: None,
                    time,
                    season: event.season as i32,
                    day_type,
                    day,
                    superstar_day,
                    player_name_before: previous,
                    player_name_after: new,
                    reverts_recomposition: None,
                });
            }
            ParsedPlayerFeedEventText::Released { .. } => {
                // There shouldn't be anything to do about this. Unlike recomposition,
                // this player's ID is retired instead of being repurposed for the
                // new player.

                // There was a bug at the start of s3 where released players weren't actually
                // released.
                if index + 1 != feed_items.len()
                    && (event.season, &event.day) != (3, &Ok(Day::Day(1)))
                {
                    // TODO Expose player ingest warnings on the site
                    ingest_logs.warn(format!(
                        "Released event wasn't the last event in the player's feed. {}/{}",
                        index + 1,
                        feed_items.len(),
                    ));
                }
            }
            ParsedPlayerFeedEventText::Modification { .. } => {
                // See comment on HitByFallingStar
            }
        }
    }

    // Apply any pending inferred whose time is before this event's time
    if let Some(pending) = &mut pending_inferred_recompositions {
        let naive_valid_from = valid_from.naive_utc();
        let mut inferred_event_index = 0;
        while let Some((time, info)) = pending.next_if(|(dt, _)| *dt <= naive_valid_from) {
            let (season, day, player_name_before, player_name_after) = info;
            ingest_logs.info(format!(
                "Applying inferred recomposition from {player_name_before} to {player_name_after}",
            ));
            check_player_name.check_or_set_name(player_name_before, &mut ingest_logs);
            check_player_name.set_known_name(player_name_after);
            let (day_type, day, superstar_day) = day_to_db(&Ok(*day), taxa);
            recompositions.push(NewPlayerRecomposition {
                mmolb_player_id: player_id,
                // feed_event_index for inferred events is the feed event index of
                // the first real feed event _after_ this inferred event. For inferred
                // events that are after the last real event, it's the past-the-end index.
                feed_event_index: max_permanent_feed_event_index_plus_one,
                inferred_event_index: Some(inferred_event_index),
                time: *time,
                season: *season,
                day_type,
                day,
                superstar_day,
                player_name_before,
                player_name_after,
                reverts_recomposition: None,
            });
            inferred_event_index += 1;
        }
    };

    check_player_name.clear_temporary_name_override();

    if let Some(name) = final_player_name {
        check_player_name.check_or_set_name(name, &mut ingest_logs);
    }

    (
        player_feed_version,
        attribute_augments,
        paradigm_shifts,
        recompositions,
        ingest_logs.into_vec(),
    )
}