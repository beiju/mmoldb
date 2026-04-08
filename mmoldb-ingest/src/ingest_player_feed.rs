use crate::config::IngestibleConfig;
use crate::ingest::{VersionIngestLogs};
use crate::ingest_players::day_to_db;
use crate::{FeedEventVersionStage1Ingest, IngestStage, Ingestable, IngestibleFromVersions, Stage2Ingest};
use chron::ChronEntity;
use chrono::{DateTime, NaiveDateTime, NaiveDate, NaiveTime, Utc};
use hashbrown::HashMap;
use itertools::Itertools;
use log::{error};
use mmolb_parsing::enums::{Attribute, Day};
use mmolb_parsing::feed_event::FeedEvent;
use mmolb_parsing::player_feed::ParsedPlayerFeedEventText;
use mmoldb_db::models::{NewFeedEventProcessed, NewPlayerAttributeAugment, NewPlayerParadigmShift, NewPlayerRecomposition, NewVersionIngestLog};
use mmoldb_db::taxa::Taxa;
use mmoldb_db::{async_db, db, AsyncPgConnection, Connection, PgConnection, QueryResult};
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use futures::Stream;
use mmolb_parsing::player::Deserialize;
use lazy_static::lazy_static;

const fn datetime_from_parts(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32, micro: u32) -> DateTime<Utc> {
    NaiveDateTime::new(
        NaiveDate::from_ymd_opt(year, month, day).unwrap(),
        NaiveTime::from_hms_micro_opt(hour, min, sec, micro).unwrap()
    ).and_utc()
}

const IGNORE_EVENTS_STARTING: DateTime<Utc> = datetime_from_parts(2026, 03, 29, 06, 53, 14, 327897);
const IGNORE_EVENTS_ENDING: DateTime<Utc> = datetime_from_parts(2026, 03, 29, 09, 10, 47, 911977);

lazy_static! {
    #[rustfmt::skip]
    static ref IGNORED_FEED_EVENTS: HashMap<(&'static str, i32), DateTime<Utc>> = {
        HashMap::from_iter(
            [
                // entity id, feed event index, timestamp of the ignored version
                // These are all impermantent recompositions
                ("6805db0cac48194de3cd40dd", 6, "2025-07-14T12:32:16.183651+00:00"),
                ("6840fa75ed58166c1895a7f3", 14, "2025-07-14T12:58:25.172157+00:00"),
                ("6840fb13e63d9bb8728896d2", 21, "2025-07-14T11:56:08.156319+00:00"),
                ("6840fe6508b7fc5e21e8a940", 9, "2025-07-14T15:04:09.335705+00:00"),
                ("6841000988056169e0078792", 11, "2025-07-14T11:58:34.656639+00:00"),
                ("684102aaf7b5d3bf791d67e8", 6, "2025-07-14T12:54:23.274555+00:00"),
                ("684102dfec9dc637cfd0cad6", 15, "2025-07-14T12:01:51.299731+00:00"),
                ("68412d1eed58166c1895ae66", 15, "2025-07-14T12:20:36.597014+00:00"),
                ("68418c52554d8039701f1c93", 12, "2025-07-14T12:09:42.764129+00:00"),
                ("6846cc4d4a488309816674ff", 13, "2025-07-14T12:10:19.571083+00:00"),
                ("684727f5bb00de6f9bb79973", 14, "2025-07-14T12:14:14.710296+00:00"),
                ("68505bc5341f7f2421020d05", 16, "2025-07-14T12:13:58.129103+00:00"),
                ("6855b350f1d8f657407b231c", 2, "2025-07-14T15:02:55.607448+00:00"),
                ("68564374acfab5652c3a6c44", 7, "2025-07-15T01:02:41.992667+00:00"),
                ("685b740338c6569da104aa48", 5, "2025-07-14T12:47:59.895679+00:00"),
                ("686355f4b254dfbaab3014b0", 2, "2025-07-14T11:51:34.711389+00:00"),
                ("68655942f27aa83a88fa64e0", 3, "2025-07-14T10:36:47.308576+00:00"),
                ("6840fb13e63d9bb8728896d2", 21, "2025-07-14T11:56:08.156319+00:00"),
                ("6845e3d4ec9dc637cfd0d38f", 11, "2025-07-14T12:01:47.540116+00:00"),
                // This is the only instance of a "X was renamed to Y" event, it was
                // overwritten after 2.5 hours, and the name smells like a mod action.
                // That seems like good enough grounds to ignore it to me.
                ("6840fb32e2d7557e153cc1c5", 35, "2025-09-01T20:51:09.010935+00:00"),
            ]
            .into_iter()
            .map(|(player_id, feed_event_index, applied_date_str)| {
                let applied_date = DateTime::parse_from_rfc3339(applied_date_str)
                    .expect("Hard-coded date must parse")
                    .to_utc();
                ((player_id, feed_event_index), applied_date)
            })
        )
    };
    #[rustfmt::skip]
    static ref OVERWRITTEN_RECOMPOSITIONS: HashMap<(&'static str, i32), (&'static str, &'static str, NaiveDateTime, i32, Day)> = {
        HashMap::from_iter(
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
                // Added by hand after the s11 changes surfaced them
                (("685ca5e2e5047bc3a0d1552e", 9), ("Nellie Phiri", "Hal Webb", "2025-08-01T04:27:56.172450+00:00", 4, Day::Day(79))),
                (("68770bb6f3a82ff1e69604ed", 3), ("Oscar Bentley", "Rafael Sampson", "2025-08-04T04:09:49.112865+00:00", 4, Day::Day(145))),
                (("687d30f00f0b0ce1ce3ecc3e", 1), ("Xiaohua Jovanovski", "O. Abe", "2025-08-05T03:56:23.335562+00:00", 4, Day::Day(168))),
                (("68797d7c942d051dd530e3b3", 9), ("Ruby Morita", "Helen Harrison", "2025-08-07T03:58:27.287405+00:00", 4, Day::Day(216))),
                (("6843ae00554d8039701f1fd6", 43), ("Ada Hämäläinen", "Jon Nkurunziza", "2025-08-07T04:01:58.841956+00:00", 4, Day::Day(217))),
                (("68426ec08d67c531e89fe649", 35), ("J. Melendez", "Huan Andreasen", "2025-08-08T04:09:07.354023+00:00", 4, Day::Day(240))),
                (("68655e2e02f61969e2ced862", 20), ("Vandium Peterson", "Melissa Lavoie", "2025-08-08T04:12:06.045493+00:00", 4, Day::Day(240))),
                (("6805db0cac48194de3cd4017", 7), ("Fred Rush", "Miguel Porter", "2025-08-12T01:16:55.053911+00:00", 4, Day::Holiday)),
                (("6805db0cac48194de3cd3ff9", 8), ("Débora Randolph", "Rowan Aung", "2025-08-12T01:40:41.803615+00:00", 4, Day::Holiday)),
                // (("68abe24b2cd8a532516a0336", 1), ("Koala Kono", "Kate Sherwood", "2025-09-01T05:09:32.514563+00:00", 5, Day::Election)),
                // Actually I went back to doing it by a query:
                // select
                //     '(("' 
                //         || fe1.entity_id 
                //         || '", ' 
                //         || fe1.feed_event_index 
                //         || '), ("' 
                //         || split_part(fe1.data->>'text', ' was Recomposed into ', 1) 
                //         || '", "' 
                //         || left(split_part(fe1.data->>'text', ' was Recomposed into ', 2), -1) 
                //         || '", "' 
                //         || (fe1.data->>'ts') 
                //         || '", ' 
                //         || (fe1.data->>'season')  
                //         || ', ' 
                //         || (case when (fe1.data->>'status') = 'Regular Season' then ('Day::Day(' 
                //         || (fe1.data->>'day') 
                //         || ')') else ('Day::' 
                //         || (fe1.data->>'day')) end) 
                //         || ')),'
                // from data.feed_event_versions fe1
                // inner join data.feed_event_versions fe2
                //     on fe1.entity_id=fe2.entity_id
                //     and fe1.feed_event_index=fe2.feed_event_index
                //     and fe1.valid_until=fe2.valid_from
                // where fe1.kind='player_feed'
                //     and fe2.kind='player_feed'
                //     and fe1.data->>'text' like '%was Recomposed into%'
                //     and fe1.data->>'ts' >= '2025-08-12T01:40:41.803615+00:00'
                //     and fe2.data->>'ts' > fe1.data->>'ts'
                //     and fe2.valid_from < '2026-03-15T09:20:20.143021Z'
                //     and fe2.data->>'text' like split_part(fe1.data->>'text', ' was Recomposed into ', 1) 
                // || '%'
                // order by fe1.data->>'ts';
                (("686313bb58c82f639e536fd6", 13), ("Jaime Bytyqi", "Willow Jorts", "2025-08-20T04:16:42.606867+00:00", 5, Day::Day(29))),
                (("6889236c570d8b89bc15a189", 1), ("Ray Kensingtonworth", "Quackers Büchel", "2025-08-20T04:25:09.553657+00:00", 5, Day::Day(29))),
                (("68462653896f631e9d6898db", 13), ("Roger Mann", "J. Healy", "2025-08-21T04:26:58.922877+00:00", 5, Day::Day(53))),
                (("6840feaee2d7557e153cc369", 32), ("Cora Jiang", "Chalia Schubert", "2025-08-21T04:41:30.100527+00:00", 5, Day::Day(53))),
                (("68437dab554d8039701f1f9f", 40), ("Kobe Johanssen", "Cesar Watkins", "2025-08-22T03:57:25.088910+00:00", 5, Day::Day(76))),
                (("689a4b872b4cf1e121db9733", 6), ("Bill Eriksen", "Paola Villanuava", "2025-08-28T04:10:52.053648+00:00", 5, Day::Day(217))),
                (("68411c85896f631e9d689283", 49), ("Cash Heinonen", "Howard de La Cruz", "2025-08-29T04:34:56.235722+00:00", 5, Day::Day(240))),
                // This is also marked as day 269, not sure how to represent that or if I should
                (("68412683183c892d88a0ffab", 28), ("Janice Girard", "Amelia Clavicle", "2025-09-01T05:01:00.112468+00:00", 5, Day::PostseasonRound(3))),
                (("685c852dd7b446d87c1014eb", 20), ("Nap Stokes", "Steven Božić", "2025-09-01T05:01:23.910657+00:00", 5, Day::Election)),
                (("68434002554d8039701f1f52", 39), ("Pascal Sawyer", "Yosef Mulder", "2025-09-01T05:02:58.200769+00:00", 5, Day::Election)),
                (("68410559ed58166c1895ac19", 33), ("Phelmo van der Veen", "Dean Ai", "2025-09-01T05:03:44.961319+00:00", 5, Day::Election)),
                (("686629b8f27aa83a88fa6517", 10), ("Phelmo Walsh", "Alewife Soares", "2025-09-01T05:03:47.730160+00:00", 5, Day::Election)),
                (("684a184df9ba16bd86033b9b", 26), ("Extra Jovanovska", "Ernie Ram", "2025-09-01T05:05:13.947266+00:00", 5, Day::Election)),
                (("6840fb60554d8039701f1465", 30), ("Kennedy Hughes", "Paul Yates", "2025-09-01T05:05:18.358853+00:00", 5, Day::Election)),
                (("684e515eef5d0745273d90d3", 27), ("Fernanda Dobson", "Abigail Trajkovski", "2025-09-01T05:06:28.387833+00:00", 5, Day::Election)),
                (("6841064f08b7fc5e21e8abce", 23), ("Heidi Barbier", "Jazzy Hansson", "2025-09-01T05:06:57.674037+00:00", 5, Day::Election)),
                (("6841320c183c892d88a10016", 20), ("Hye-Jin Bouchard", "Antonio Lomidze", "2025-09-01T05:07:57.783544+00:00", 5, Day::Election)),
                (("6840fde6183c892d88a0fba0", 35), ("Blip Walton", "Cassette Everett", "2025-09-01T05:09:03.721018+00:00", 5, Day::Election)),
                (("68abe24b2cd8a532516a0336", 1), ("Koala Kono", "Kate Sherwood", "2025-09-01T05:09:32.514563+00:00", 5, Day::Election)),
                (("688973b0ebf08e853913b8f5", 19), ("Test Machado", "Akira James", "2025-09-01T05:10:35.652038+00:00", 5, Day::Election)),
                (("68410f7c54a7fbd4133872db", 23), ("Fred Orou", "Jo Sun", "2025-09-01T05:11:04.009897+00:00", 5, Day::Election)),
                (("6840ffb7554d8039701f16cd", 19), ("Ken Guthrie", "Pogo-Stick Arai", "2025-09-01T05:11:22.540191+00:00", 5, Day::Election)),
                (("687bc4739cdd68529b86be74", 14), ("Lucille Otto", "Irene Bernandez", "2025-09-01T05:12:03.273320+00:00", 5, Day::Election)),
                (("6845eb4508b7fc5e21e8b36e", 34), ("Velma Wang", "Fauna Bah", "2025-09-01T05:13:12.047024+00:00", 5, Day::Election)),
                (("6841057c8ba62cf2ce8d8fe0", 20), ("Antony Saelim", "Garnet Hailu", "2025-09-01T05:13:57.279716+00:00", 5, Day::Election)),
                (("68410a79e63d9bb872889bbb", 18), ("Leila Rosales", "Hank Monroe", "2025-09-01T05:14:08.187522+00:00", 5, Day::Election)),
                (("684148c8183c892d88a1009b", 22), ("Rina Feller", "James Mikkelsen", "2025-09-01T05:19:01.804252+00:00", 5, Day::Election)),
                (("684118c4925dd4f9d72ac0da", 38), ("Chorby Campesinos", "Sam Genet", "2025-09-01T05:20:14.689214+00:00", 5, Day::Election)),
                (("68427df8144e874e6deb8aa8", 19), ("Brick Javier", "Ana Carolina Sharpe", "2025-09-01T05:24:54.237349+00:00", 5, Day::Election)),
                (("68651c2ff27aa83a88fa64d5", 6), ("Clearly Oya", "Oakley Winter", "2025-09-01T05:29:17.760611+00:00", 5, Day::Election)),
                (("6886a02c0a4902fcc3c63e84", 12), ("Julia Klymenko", "Gargle Vasquez", "2025-09-01T05:32:47.418461+00:00", 5, Day::Election)),
                (("6841450b144e874e6deb877d", 17), ("Carlton Atkins", "Kehlani Badilla", "2025-09-01T05:32:52.535888+00:00", 5, Day::Election)),
                (("6886ff951f79b00a978a66fa", 4), ("Camila Lorenzo", "Gustavo Nuñez", "2025-09-01T05:49:26.841477+00:00", 5, Day::Election)),
                (("684102a2183c892d88a0fd2d", 37), ("Ozzie Bannister", "Wilma Turnbull", "2025-09-01T05:55:37.602841+00:00", 5, Day::Election)),
                (("686b24ef77149281c81eaa69", 15), ("Yahoo Barbieri", "Ball Yun", "2025-09-01T06:02:17.738119+00:00", 5, Day::Election)),
                (("68797de5dad0efab0b8916a2", 22), ("Stella Bois", "Ivan Nakajima", "2025-09-01T06:04:32.069006+00:00", 5, Day::Election)),
                (("687e45303f76efa467642cee", 15), ("Luciana Tyler", "Patsy Junior", "2025-09-01T06:05:11.137498+00:00", 5, Day::Election)),
                (("68414305e63d9bb872889e73", 22), ("Evelyn Seki", "Nelson Bender", "2025-09-01T06:30:18.403578+00:00", 5, Day::Election)),
                (("6840fb18ec9dc637cfd0c7b4", 23), ("Andrea Feller", "Suzanne Ramirez", "2025-09-01T06:41:16.691624+00:00", 5, Day::Election)),
                (("6841f775144e874e6deb89c8", 34), ("Goose Bond", "Terri Frye", "2025-09-01T06:42:51.607101+00:00", 5, Day::Election)),
                (("6842c17408b7fc5e21e8b1c3", 32), ("Brenda Claeys", "Della Rain", "2025-09-01T07:03:59.769796+00:00", 5, Day::Election)),
                (("684170d5295b2368c0ac799b", 19), ("Luciana Kovalchuk", "Tracy Olsson", "2025-09-01T07:04:49.441141+00:00", 5, Day::Election)),
                (("6840fd8f925dd4f9d72abcdd", 22), ("Ronnie Sherman", "Bee Murray", "2025-09-01T07:10:34.724742+00:00", 5, Day::Election)),
                (("6850607411cffaffe201c42c", 38), ("Isaiah Bos", "Boots Ashley", "2025-09-01T07:11:31.164174+00:00", 5, Day::Election)),
                (("6840fb15554d8039701f141e", 37), ("Messiah Wallis", "Marcela Hewitt", "2025-09-01T07:35:43.027425+00:00", 5, Day::Election)),
                (("68865ae131a33266cc23450b", 15), ("Kay Ellis", "Jamie Anthony", "2025-09-01T07:43:26.850745+00:00", 5, Day::Election)),
                (("689eeac12892ac158e1f3f20", 2), ("Yoon-Jae Meadows", "Antony Doubles", "2025-09-01T07:59:42.801402+00:00", 5, Day::Election)),
                (("68757367be6293c111015beb", 31), ("Rita Sello", "Kimmie Clemens", "2025-09-01T08:45:29.708414+00:00", 5, Day::Election)),
                (("6844a8e0ed58166c1895b326", 30), ("Ariella Campesinos", "Ryder Collins", "2025-09-01T09:35:07.582539+00:00", 5, Day::Election)),
                (("6888dde542628aac654935fd", 9), ("Yuki Spence", "Matteo Matsuda", "2025-09-01T09:48:24.863241+00:00", 5, Day::Election)),
                (("6887e0909f55cf65fce7b0e1", 13), ("Valerie Solano", "Christina Petersen", "2025-09-01T10:06:34.971437+00:00", 5, Day::Election)),
                (("6841489d183c892d88a10096", 27), ("May Ramierz", "Arturo Fortin", "2025-09-01T10:31:09.097150+00:00", 5, Day::Election)),
                (("6867ae0677149281c81ea9cf", 29), ("Waddle Cain", "Carwyn Galbraith", "2025-09-01T10:36:11.715037+00:00", 5, Day::Election)),
                (("684ea72a3ee1833536bdcdcf", 23), ("Seung-Ho Szymański", "Carrie Radu", "2025-09-01T10:44:43.612256+00:00", 5, Day::Election)),
                (("68abc77f1ca373c5301a4fe4", 3), ("Laisa Tamm", "Brandi Byun", "2025-09-01T10:45:14.409852+00:00", 5, Day::Election)),
                (("68aca39ab3d15cd034b0a292", 0), ("Brick Bonilla", "Ford Wilkins", "2025-09-01T11:58:46.125273+00:00", 5, Day::Election)),
                (("6841026e183c892d88a0fd18", 41), ("Beulah Olsen", "Ashley Kirk", "2025-09-01T12:09:28.836101+00:00", 5, Day::Election)),
                (("68aa26f8d38a4941f545e01f", 0), ("Kim Hampton", "Carrie Carrington", "2025-09-01T12:26:27.483842+00:00", 5, Day::Election)),
                (("688916ab5cb20f9e396ef6ac", 12), ("Adalynn Schmitt", "Herbert König", "2025-09-01T12:29:44.008359+00:00", 5, Day::Election)),
                (("6840feb954a7fbd413386fc9", 17), ("Blip Spencer", "Jackie Nikolaou", "2025-09-01T12:35:28.102671+00:00", 5, Day::Election)),
                (("6845947bf7b5d3bf791d6f66", 31), ("Cash Myers", "Linda Kolesnyk", "2025-09-01T12:44:52.300179+00:00", 5, Day::Election)),
                (("6889008f42628aac65493611", 5), ("Carlton Vasilev", "Jacqueline Tomlinson", "2025-09-01T12:51:32.431292+00:00", 5, Day::Election)),
                (("68a9eab9455749140e890a25", 10), ("Tyler Kraus", "Denise Patton", "2025-09-01T12:52:05.121688+00:00", 5, Day::Election)),
                (("689a4e0621e8c4c568fd12e6", 10), ("Yuto Iversen", "Twilight Nkhoma", "2025-09-01T12:54:04.238586+00:00", 5, Day::Election)),
                (("689547059c43e94f152e7f16", 4), ("Phyllis Charles", "Pluto Trajkovski", "2025-09-01T12:55:02.912008+00:00", 5, Day::Election)),
                (("686702f24dca64cf777cec8e", 8), ("Carlos Wyldarms", "Cal Sultana", "2025-09-01T14:14:13.003207+00:00", 5, Day::Election)),
                (("6854a61eded70fd592b811ab", 28), ("Lisbon Kolesnyk", "Mattie Reeves", "2025-09-01T14:15:08.247042+00:00", 5, Day::Election)),
                (("684763b05fc0f93e7fa38e0b", 17), ("Yasmin Hudson", "Crumble Lund", "2025-09-01T14:50:39.287810+00:00", 5, Day::Election)),
                (("684103af88056169e00788a5", 40), ("Qingyu Yamauchi", "Ched Vargová", "2025-09-01T15:26:04.984878+00:00", 5, Day::Election)),
                (("6841189854a7fbd41338736b", 17), ("Nikolai Olson", "Eggo McMahon", "2025-09-01T15:33:34.266718+00:00", 5, Day::Election)),
                (("689da525760242d10ffddc9d", 7), ("Maëlys Bright", "Judy Nuñez", "2025-09-01T15:53:12.148202+00:00", 5, Day::Election)),
                (("6861728a60b66eb2d94d4fdc", 22), ("Erica Field", "Satchel Harringtonshire", "2025-09-01T16:14:04.382712+00:00", 5, Day::Election)),
                (("68545056875d2d473526cf14", 24), ("Rick Brito", "Midnight Phelps", "2025-09-01T16:49:30.221353+00:00", 5, Day::Election)),
                (("6840fc9fe63d9bb8728897dd", 18), ("Internet Hazell", "Harrietta Wolff", "2025-09-01T16:54:01.356686+00:00", 5, Day::Election)),
                (("684144ec88056169e0078bba", 20), ("Camila Tyler", "Ganymede Roy", "2025-09-01T16:55:27.299103+00:00", 5, Day::Election)),
                (("684466ee88056169e0078fc1", 21), ("Edward Cheng", "Bus Wilcox", "2025-09-01T17:29:43.757649+00:00", 5, Day::Election)),
                (("684335e18ba62cf2ce8d961f", 15), ("Trevor Cranston", "Christie Hori", "2025-09-01T17:39:05.045305+00:00", 5, Day::Election)),
                (("685e60b11c30cdd288cde4d3", 12), ("Carlton Northrop", "Shawn Aston", "2025-09-01T17:48:57.433121+00:00", 5, Day::Election)),
                (("68419aa654a7fbd41338762a", 41), ("Nice Leblanc", "Bubblegum Butler", "2025-09-01T17:59:35.229021+00:00", 5, Day::Election)),
                (("68751e061b5fc01a8ac50a8a", 13), ("Stanley Jiminez", "Mia Zin", "2025-09-01T18:22:38.695331+00:00", 5, Day::Election)),
                (("684100e2554d8039701f171e", 31), ("Kelly Viña", "Audrey Marino", "2025-09-01T18:24:08.637726+00:00", 5, Day::Election)),
                (("68410b3e925dd4f9d72abff0", 37), ("Jean Jackson", "Muffin Lacroix", "2025-09-01T19:11:03.130316+00:00", 5, Day::Election)),
                (("684109f38d67c531e89fe148", 18), ("Aaron Laplace", "Liam Singh", "2025-09-01T19:20:25.720171+00:00", 5, Day::Election)),
                (("685e26fb3cb1052ee36a9fad", 22), ("Tainá Acuña", "Alma Dunne", "2025-09-01T20:38:17.329036+00:00", 5, Day::Election)),
                (("68a0d4a22892ac158e1f3f42", 1), ("Wei Nakata", "Tonya Koch", "2025-09-01T21:25:20.219808+00:00", 5, Day::Election)),
                (("6805db0cac48194de3cd40d2", 15), ("KW Cuffy", "Midnight McCann", "2025-09-01T21:41:43.822626+00:00", 5, Day::Election)),
                (("6875c99f4d21598a0f77f5b3", 18), ("Omari Hart", "Sara Frye", "2025-09-01T21:53:14.816981+00:00", 5, Day::Election)),
                (("68472c173a29cec29f263c85", 32), ("Fatima Cherry", "Ana Carolina James", "2025-09-01T22:30:44.541417+00:00", 5, Day::Election)),
                (("68885f7997f2b5415c1128f3", 7), ("T. Tanner", "N. Lopez", "2025-09-01T23:45:15.196107+00:00", 5, Day::Election)),
                (("6840fc61925dd4f9d72abc12", 20), ("Selena Fairchild", "Count Anthony", "2025-09-02T00:35:15.187858+00:00", 5, Day::Election)),
                (("68a9378b1dcebdc270ab59fb", 4), ("Lloyd Langley", "Aurora Carr", "2025-09-02T00:49:16.560645+00:00", 5, Day::Election)),
                (("6844a91f54a7fbd4133879f4", 31), ("February Genet", "Zack Valenzuela", "2025-09-06T16:56:57.531259+00:00", 5, Day::Offseason)),
                (("684104d1554d8039701f1821", 37), ("Richard Lozano", "Gina Silva", "2025-09-07T22:11:50.149654+00:00", 5, Day::Offseason)),
                (("68b513be937f58069c985d87", 4), ("Gnav de Souza", "Kyrie Paredes", "2025-09-08T23:31:18.937509+00:00", 5, Day::Offseason)),
                (("68417662295b2368c0ac79bd", 13), ("Makena Jansson", "Faux Whosonfirst", "2025-09-11T00:37:00.904859+00:00", 5, Day::Offseason)),
                (("6843a78b183c892d88a1044b", 44), ("Shawn Mathieu", "Nelson Kaya", "2025-09-11T02:39:28.326449+00:00", 5, Day::Offseason)),
                (("68b2346770fc4ca5bcb63d8a", 5), ("Brody Every", "Perseverance Moser", "2025-09-12T01:36:48.659153+00:00", 5, Day::Offseason)),
                (("68c6128e5c5459140c736d70", 0), ("Ty Brady", "S. Bradshaw", "2025-09-15T18:49:12.890385+00:00", 5, Day::Offseason)),
                (("684172e9925dd4f9d72ac2d4", 21), ("Jocelyn Kelly", "Jack Wang", "2025-09-17T03:48:30.356538+00:00", 5, Day::Offseason)),
                // TODO: This is an offseason day with day number 996, which we
                //   know because feed events have both a 'day' and a 'status'.
                //   This isn't deserializable from just the 'day' field, so
                //   the Day enum doesn't support it. The task is to figure
                //   this out somehow.
                (("68d5e9adfb606a651236fcfa", 3), ("Jennifer Johansen", "Humongous Haruna", "2025-10-05T18:00:56.342042+00:00", 5, Day::Day(996))),
                (("6840fdc38d67c531e89fde65", 28), ("Carrie Gutierrez", "Enrique Vargas", "2025-10-10T04:36:06.754574+00:00", 6, Day::Day(23))),
                (("6850dae61b474e53619873ce", 26), ("Etheridge Badilla", "Pearu Carew", "2025-10-11T03:58:40.971098+00:00", 6, Day::Day(40))),
                (("68658cbd77149281c81ea93c", 18), ("Huan O'Connor", "Carolina Stojanović", "2025-10-14T04:53:26.656523+00:00", 6, Day::Day(113))),
                (("68472a73fc876482fd5f9b7e", 33), ("Courtney Lake", "Albert Yusupova", "2025-10-14T13:30:50.549424+00:00", 6, Day::Day(120))),
                (("68ec5452d834c6992b5c52b7", 0), ("Cal Salinas", "Uncle Carlsen", "2025-10-14T23:28:21.143234+00:00", 6, Day::Day(120))),
                (("6840fc1e144e874e6deb8207", 47), ("R. Baxter", "Christy Vanterpool", "2025-10-18T04:07:05.558477+00:00", 6, Day::Day(169))),
                (("6843449d925dd4f9d72ac5f2", 48), ("Randall Brennan", "Finnegan Yamada", "2025-10-18T04:11:07.294553+00:00", 6, Day::Day(169))),
            ]
            .into_iter()
            .map(|(key, (nb, na, dt, s, d))| (key, (nb, na, DateTime::parse_from_rfc3339(dt).unwrap().naive_utc(), s, d)))
        )
    };

    #[rustfmt::skip]
    static ref INFERRED_RECOMPOSITIONS: HashMap<&'static str, Vec<(NaiveDateTime, (i32, Day, &'static str, &'static str))>> = {
        // Some apparent recompositions happened without a feed event
        HashMap::from_iter(
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
                // The ones below appear to be in the feed just fine, not sure where I got them from
                // ("6879d391762fe1c1e4769883", [
                //     ("2025-07-26T00:42:41.224451Z", (3, Day::PostseasonPreview, "Han-Soo van der Meer", "Tommy van den Berg"))
                // ]),
                // ("6840feff8ba62cf2ce8d8e41", [
                //     ("2025-07-25T18:14:08.672137Z", (3, Day::PostseasonPreview, "Asher Boyd", "Algernon Welch"))
                // ]),
                // ("684339118ba62cf2ce8d9620", [
                //     ("2025-07-26T09:50:09.471896Z", (3, Day::PostseasonPreview, "Dilly-Dally Sarkar", "July Shah"))
                // ]),
                // ("687e44b6a18a39dc5d280cb8", [
                //     ("2025-07-26T06:06:08.474301Z", (3, Day::PostseasonPreview, "Hobble Schulz", "Chorby Schneider"))
                // ]),
                // ("687c4009af79e504eb851455", [
                //     ("2025-07-25T21:24:49.851079Z", (3, Day::PostseasonPreview, "Neptune Grgić", "Boyfriend Otsuka"))
                // ]),
                // ("68412307896f631e9d6892c8", [
                //     ("2025-07-26T01:33:02.907220Z", (3, Day::PostseasonPreview, "Ahmed Nunes", "Francisco Langston"))
                // ]),
                // ("6847d0c44a488309816676d6", [
                //     ("2025-07-25T22:21:19.095142Z", (3, Day::PostseasonPreview, "Dawn Injury", "Georgia Dracula"))
                // ]),
                // ("684102fa183c892d88a0fd3a", [
                //     ("2025-07-25T20:05:23.830022Z", (3, Day::PostseasonPreview, "Esther Carlington", "Ally Mashaba"))
                // ]),
                // ("68751f7fc1f9dc22d3a8f267", [
                //     ("2025-07-26T00:44:53.626041Z", (3, Day::PostseasonPreview, "Hitchcock Whitaker", "Mamie Ling"))
                // ]),
                // ("6875b958fa3eb3ff9c319ec4", [
                //     ("2025-07-25T19:43:36.410787Z", (3, Day::PostseasonPreview, "Pogo-Stick Curran", "Maybelline Lehtinen"))
                // ]),
                // ("68410cb088056169e0078998", [
                //     ("2025-07-26T05:03:28.822305Z", (3, Day::PostseasonPreview, "Chloe Alfonso", "Fannie Oconnor"))
                // ]),
            ]
            .into_iter()
            .map(|(id, recompositions)| {
                    (
                        id,
                        recompositions.into_iter()
                            .map(|(dt, names)| {
                                (DateTime::parse_from_rfc3339(dt).unwrap().naive_utc(), names)
                            })
                            .collect()
                    )
                })
        )
    };
}

#[derive(Deserialize)]
pub struct PlayerFeedItemContainer {
    feed_event_index: i32,
    data: FeedEvent,
    prev_valid_from: Option<DateTime<Utc>>,
    prev_data: Option<FeedEvent>,
}

pub struct PlayerFeedIngestFromVersions;

impl IngestibleFromVersions for PlayerFeedIngestFromVersions {
    type Entity = PlayerFeedItemContainer;

    fn get_start_cursor(_: &mut PgConnection) -> QueryResult<Option<(NaiveDateTime, String)>> {
        // TODO: This is None because I'm not using a cursor for player feeds any more. Update
        //   the infrastructure to not require a stub.
        Ok(None)
    }

    fn trim_unused(version: &serde_json::Value) -> serde_json::Value {
        version.clone()
    }

    fn insert_batch(conn: &mut PgConnection, taxa: &Taxa, versions: &Vec<ChronEntity<Self::Entity>>) -> QueryResult<(usize, usize)> {
        let new_versions = versions.iter()
            .map(|player| chron_player_feed_as_new(taxa, &player.entity_id, player.valid_from, &player.data, None))
            .collect_vec();

        conn.transaction(|c| {
            db::insert_player_feed_versions(c, &new_versions)
        })
    }

    async fn stream_versions_at_cursor(
        conn: &mut AsyncPgConnection,
        kind: &str,
        _: Option<(NaiveDateTime, String)>,
    ) -> QueryResult<impl Stream<Item=QueryResult<ChronEntity<serde_json::Value>>>> {
        // This ingestible doesn't use a cursor. I used to have an assert that cursor
        // was None, but that's incorrect because the machinery opportunistically updates
        // the cursor based on values that are passing through
        async_db::stream_unprocessed_feed_event_versions(conn, kind).await
    }
}

pub struct PlayerFeedIngest(&'static IngestibleConfig);

impl PlayerFeedIngest {
    pub fn new(config: &'static IngestibleConfig) -> PlayerFeedIngest {
        PlayerFeedIngest(config)
    }
}

impl Ingestable for PlayerFeedIngest {
    const KIND: &'static str = "player_feed";

    fn config(&self) -> &'static IngestibleConfig {
        &self.0
    }

    fn stages(&self) -> Vec<Arc<dyn IngestStage>> {
        vec![
            Arc::new(FeedEventVersionStage1Ingest::new(Self::KIND, "player")),
            Arc::new(Stage2Ingest::new(Self::KIND, PlayerFeedIngestFromVersions))
        ]
    }
}

fn process_paradigm_shift<'e>(
    changing_attribute: Attribute,
    value_attribute: Attribute,
    feed_event_index: i32,
    event: &FeedEvent,
    time: NaiveDateTime,
    player_id: &'e str,
    taxa: &Taxa,
) -> Option<NewPlayerParadigmShift<'e>> {
    if changing_attribute == Attribute::Priority {
        let (day_type, day, superstar_day) = day_to_db(Some(&event.day), taxa);

        Some(NewPlayerParadigmShift {
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
        None
    }
}

pub fn chron_player_feed_as_new<'a>(
    taxa: &Taxa,
    player_id: &'a str,
    valid_from: DateTime<Utc>,
    event: &'a PlayerFeedItemContainer,
    final_player_name: Option<&str>,
) -> (
    NewFeedEventProcessed<'a>,
    Option<NewPlayerAttributeAugment<'a>>,
    Option<NewPlayerParadigmShift<'a>>,
    Vec<NewPlayerRecomposition<'a>>,
    Vec<NewVersionIngestLog<'a>>,
) {
    let mut ingest_logs = VersionIngestLogs::new(PlayerFeedIngest::KIND, player_id, valid_from);

    let processed = NewFeedEventProcessed {
        kind: "player_feed",
        entity_id: player_id,
        feed_event_index: event.feed_event_index,
        valid_from: valid_from.naive_utc(),
    };

    if IGNORE_EVENTS_STARTING <= valid_from && valid_from <= IGNORE_EVENTS_ENDING {
        // At the beginning of s11, the feed endpoint (a) started paginating
        // and (b) reversed the order of feed events. This was done in a
        // backwards-compatible way, so all the infrastructure kept ingesting
        // events just fine, but since the indices all changed it was
        // recognized as a new version of every event.
        //
        // We fixed this after a few hours, by adding special handling to feeds
        // to restore the order, but there were still two erroneous versions
        // generated for many events: one where the indices changed from the
        // api change, and one when they changed back from the fix. Our
        // solution for the first group is to ignore them by date, and for the
        // second group we just ingest them twice and let the database's
        // duplicate handling deal with it.
        //
        // Note that we can't just ignore the second group because some new
        // feed events were genered during the Inversion, and we can't ingest
        // them during the Inversion because we don't know what their correct
        // index is. The only way to handle them correctly is to ingest them
        // when we see their post-Inversion fix event.
        //
        // TODO: Before addressing the below TODO, figure out why so many
        //   events aren't in the feed_event_versions table and fix it.
        //
        // TODO: There may be some new events generated during the Inversion
        //   that don't have a post-inversion fix event because their index
        //   didn't change (due to them being directly in the middle of the
        //   feed). The only way I can think to handle them is to hard-code
        //   them.
        // Query:
        //     select *
        //     from data.feed_event_versions fev
        //     -- Find previous versions of events (to be excluded by null check)
        //     left join data.feed_event_versions pfev
        //         on fev.kind=pfev.kind
        //         and fev.entity_id=pfev.entity_id
        //         and fev.valid_from>pfev.valid_from
        //         and fev.data->'ts'=pfev.data->'ts'
        //     where fev.valid_from >= '2026-03-29T06:53:14.327897Z'
        //         and fev.valid_from <= '2026-03-29T09:10:47.911977Z'
        //         and fev.valid_until is null
        //         and pfev.data is null -- Exclude events that have a previous version
        //     order by fev.valid_from
        ingest_logs.info("Ignoring event version from the Feed Inversion Event");

        return (processed, None, None, Vec::new(), ingest_logs.into_vec());
    }

    if let Some(prev_event) = &event.prev_data {
        if event.prev_valid_from.is_none() {
            ingest_logs.warn(format!(
                "Player {} feed event index {} had a previous event, but \
                did not have prev_valid_from",
                player_id,
                event.feed_event_index,
            ));
        }

        if IGNORED_FEED_EVENTS
            .get(&(player_id, event.feed_event_index))
            .is_some_and(|dt| dt == &prev_event.timestamp) {
            ingest_logs.info(format!(
                "Player {} feed event index {} had a previous event, but it was an \
                ignored event so we can proceed as if it doesn't exist",
                player_id,
                event.feed_event_index,
            ));
        } else if OVERWRITTEN_RECOMPOSITIONS
            .get(&(player_id, event.feed_event_index))
            .is_some_and(|(_, _, dt, _, _)| dt == &prev_event.timestamp.naive_utc()) {
            ingest_logs.info(format!(
                "Player {} feed event index {} had a previous event, but it was an \
                overwritten recomposition, so we can proceed with processing this event",
                player_id,
                event.feed_event_index,
            ));
        } else if event.prev_valid_from.is_none_or(|prev_valid_from| IGNORE_EVENTS_STARTING <= prev_valid_from && prev_valid_from <= IGNORE_EVENTS_ENDING) {
            if event.prev_valid_from.is_none() {
                ingest_logs.warn(format!(
                    "Can't check whether player {} feed event index {}'s previous event \
                    was from the Feed Inversion Event because it's missing prev_valid_from. \
                    Assuming it was to avoid losing data.",
                    player_id,
                    event.feed_event_index,
                ));
            } else {
                ingest_logs.info(format!(
                    "Player {} feed event index {} had a previous event, but it was from \
                    the Feed Inversion Event so we can proceed with processing this event",
                    player_id,
                    event.feed_event_index,
                ));
            }
        } else if event.data.timestamp == prev_event.timestamp && event.data.text == prev_event.text {
            // I'm not early-exiting here because we don't check all the
            // fields, so this could incorrectly match an actually meaningful
            // change. If that happens, the database layer checks will find it.
        ingest_logs.info(format!(
                "Player {} feed event index {} had a previous event, but it's identical \
                in text and timestamp to this event. Assuming it's just a data format change\
                and that the database will deduplicate it.",
                player_id,
                event.feed_event_index,
            ));
        } else {
            ingest_logs.error(format!(
                "Player {} feed event index {} had a previous version without special \
                handling. Skipping this version.\n\
                previous version text: {}\n\
                previous version valid_from: {}\n\
                this version text: {}\n\
                this version valid_from: {}",
                player_id,
                event.feed_event_index,
                prev_event.text,
                if let Some(dt) = event.prev_valid_from { format!("{dt}") } else { "(missing)".to_string() },
                event.data.text,
                valid_from,
            ));

            return (processed, None, None, Vec::new(), ingest_logs.into_vec());
        }
    }

    fn name_matches(_a: &str, _b: &str) -> bool {
        // Multiple Stanleys Demir were generated during the s2
        // Falling Stars event and then Danny manually renamed them
        // a == b || (a.starts_with("Stanley Demir") && b.starts_with("Stanley Demir"))
        true // TODO Re-enable or delete this
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
            Self {
                known_name: None,
                temporary_name_override: None,
            }
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
                None => {
                    write!(f, "unknown player name")
                }
                Some(name) => {
                    write!(f, "'{name}'")
                }
            }
        }
    }

    // Note that the name at the start of the feed is not (necessarily) final_player_name
    let mut check_player_name = CheckPlayerName::new();

    let mut attribute_augment = None;
    let mut paradigm_shift = None;
    // A single event can have an implied and a real recomposition
    let mut recompositions = Vec::new();

    let mut pending_inferred_recompositions = INFERRED_RECOMPOSITIONS
        .get(player_id)
        .map(|r| r.into_iter().peekable());

    let time = event.data.timestamp.naive_utc();

    check_player_name.clear_temporary_name_override();

    let mut inferred_event_index = 0;
    if let Some(recompose_dt) = IGNORED_FEED_EVENTS.get(&(player_id, event.feed_event_index)) {
        // TODO Add this as a pair of recompose/unrecompose events, so the attributes
        //   in between are accurate. Note: The date of unrecompose is NOT the date
        //   of the event that replaces this one.
        if recompose_dt == &event.data.timestamp {
            ingest_logs.info(format!(
                "Skipping feed event \"{}\" because it's hard-coded as an ignored event",
                event.data.text,
            ));
            return (
                processed,
                attribute_augment,
                paradigm_shift,
                recompositions,
                ingest_logs.into_vec(),
            );
        }
    }

    // This will *almost* always be equal to feed_items.len(), but not
    // when there is an impermanent event
    // TODO Am I handling this right since I moved to single-event ingest?
    let max_permanent_feed_event_index_plus_one = event.feed_event_index + 1;

    // Apply any inferred recompositions whose time is before this event's time
    // TODO Some way to not re-add these every time. The DB should dedup them but it's
    //   a bunch of unnecessary work.
    if let Some(pending) = &mut pending_inferred_recompositions {
        while let Some((time, info)) = pending.next_if(|(dt, _)| *dt <= time) {
            let (season, day, player_name_before, player_name_after) = info;
            ingest_logs.info(format!(
                "Applying inferred recomposition from {} to {}",
                player_name_before, player_name_after
            ));
            check_player_name.check_or_set_name(player_name_before, &mut ingest_logs);
            check_player_name.set_known_name(player_name_after);
            let (day_type, day, superstar_day) = day_to_db(Some(&Ok(*day)), taxa);
            recompositions.push(NewPlayerRecomposition {
                mmolb_player_id: player_id,
                feed_event_index: event.feed_event_index,
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

    let parsed_event = mmolb_parsing::player_feed::parse_player_feed_event(&event.data);
    let skip_this_event =
        if let Some(info) = OVERWRITTEN_RECOMPOSITIONS.get(&(player_id, event.feed_event_index)) {
            let (player_name_before, player_name_after, recompose_time, season, day) = info;

            check_player_name.check_or_set_name(player_name_before, &mut ingest_logs);
            // There shouldn't be any subsequent events, but there is still
            // the player_final_name check so we do have to update the name
            check_player_name.set_known_name(player_name_after);
            ingest_logs.debug(format!("Set expected player name to {player_name_after}"));

            let (day_type, day, superstar_day) = day_to_db(Some(&Ok(*day)), taxa);

            if let ParsedPlayerFeedEventText::Recomposed { new, previous } = &parsed_event {
                // The first time we hit one of these, it'll be a Recompose event
                ingest_logs.debug(format!(
                    "Inserting implied Recomposed event for overwritten Recompose from \
                    {player_name_before} to {player_name_after}",
                ));
                recompositions.push(NewPlayerRecomposition {
                    mmolb_player_id: player_id,
                    feed_event_index: event.feed_event_index,
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
                // I want to make sure inferred_event_index is correct if another use of it is added
                #[allow(unused_assignments)]
                {
                    inferred_event_index += 1
                };
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
                        event.data.timestamp.naive_utc(),
                    ));
                }
                true
            } else {
                // If we got here, then a previous iteration must have inserted the previous
                // inferred event
                inferred_event_index += 1;
                ingest_logs.debug(format!(
                    "Inserting unrecompose event to reset {player_name_after}'s attributes",
                ));
                // If the event in question is *not* a recompose event, that means this is the
                // event that reverted the recompose. We need to insert the unrecompose event.
                recompositions.push(NewPlayerRecomposition {
                    mmolb_player_id: player_id,
                    feed_event_index: event.feed_event_index,
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
                {
                    inferred_event_index += 1
                };

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
            "Skipping event \"{}\" because `skip_this_event` told us to",
            event.data.text,
        ));
        return (
            processed,
            attribute_augment,
            paradigm_shift,
            recompositions,
            ingest_logs.into_vec(),
        );
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
        ParsedPlayerFeedEventText::AttributeChanges {
            player_name,
            attribute,
            amount,
        } => {
            let (day_type, day, superstar_day) = day_to_db(Some(&event.data.day), taxa);

            check_player_name.check_or_set_name(player_name, &mut ingest_logs);
            attribute_augment = Some(NewPlayerAttributeAugment {
                mmolb_player_id: player_id,
                feed_event_index: event.feed_event_index,
                time,
                season: event.data.season as i32,
                day_type,
                day,
                superstar_day,
                attribute: taxa.attribute_id(attribute.into()),
                value: amount as i32,
            });
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
            paradigm_shift = process_paradigm_shift(
                changing_attribute,
                value_attribute,
                event.feed_event_index,
                &event.data,
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
        ParsedPlayerFeedEventText::FallingStarOutcome { .. } => {
            // We don't (yet) have a use for this event.
            // We can use some variants to backdate certain mod changes, but
            // the utility of doing that is limited for the same reason
            // as the utility of processing Delivery is limited.
        }
        ParsedPlayerFeedEventText::Recomposed { new, previous } => {
            let (day_type, day, superstar_day) = day_to_db(Some(&event.data.day), taxa);

            check_player_name.check_or_set_name(previous, &mut ingest_logs);
            check_player_name.set_known_name(new);
            recompositions.push(NewPlayerRecomposition {
                mmolb_player_id: player_id,
                feed_event_index: event.feed_event_index,
                inferred_event_index: None,
                time,
                season: event.data.season as i32,
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

            // I used to warn if there were any events after the Released, but
            // there's been too many bugs with events after Released and I got
            // tired of excluding them one by one.
        }
        ParsedPlayerFeedEventText::Retirement { .. } => {
            // I used to warn if anything's happened to this player since they were
            // Retired, but there's too many things that happen anyway (durability
            // loss and duplicate retirement for a start)
        }
        ParsedPlayerFeedEventText::Modification { .. } => {
            // See comment on HitByFallingStar
        }
        ParsedPlayerFeedEventText::DoorPrize { .. } => {
            // See comment on Delivery
        }
        // These events have no action
        ParsedPlayerFeedEventText::SeasonalDurabilityLoss { .. } |
        ParsedPlayerFeedEventText::CorruptedByWither { .. } |
        ParsedPlayerFeedEventText::Purified { .. } |
        ParsedPlayerFeedEventText::Party { .. } |
        ParsedPlayerFeedEventText::PlayerContained { .. } |
        ParsedPlayerFeedEventText::PlayerPositionsSwapped { .. } |
        ParsedPlayerFeedEventText::PlayerGrow { .. } |
        ParsedPlayerFeedEventText::GreaterAugment { .. } |
        ParsedPlayerFeedEventText::RetractedGreaterAugment { .. } |
        ParsedPlayerFeedEventText::RetroactiveGreaterAugment { .. } |
        ParsedPlayerFeedEventText::PlayerRelegated { .. } |
        ParsedPlayerFeedEventText::PlayerMoved { .. } |
        ParsedPlayerFeedEventText::PlayerGrewInEfflorescence { .. } |
        ParsedPlayerFeedEventText::PlayerEffloresce { .. } |
        ParsedPlayerFeedEventText::Restyle { .. } |
        ParsedPlayerFeedEventText::Augment { .. } |
        ParsedPlayerFeedEventText::BoonRecombobulated { .. } |
        ParsedPlayerFeedEventText::PlayersSwapped { .. } |
        ParsedPlayerFeedEventText::ConsumptionContestToPlayer { .. } |
        ParsedPlayerFeedEventText::ConsumptionContestToTeam { .. } |
        ParsedPlayerFeedEventText::PlayerReflected { .. } |
        ParsedPlayerFeedEventText::ElectionAppliedLevelUps { .. } => {}
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
            let (day_type, day, superstar_day) = day_to_db(Some(&Ok(*day)), taxa);
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

    // TODO Add some separate processing for name checks, because this never runs any more
    check_player_name.clear_temporary_name_override();

    if let Some(name) = final_player_name {
        check_player_name.check_or_set_name(name, &mut ingest_logs);
    }

    (
        processed,
        attribute_augment,
        paradigm_shift,
        recompositions,
        ingest_logs.into_vec(),
    )
}
