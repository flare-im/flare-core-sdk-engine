//! DoH 查询：RFC 8484 的 GET 形式，所有 DoH 解析器都支持。两种用途：
//!
//! - TXT 指针（T4）：指针不需要签名，它只提供「去哪儿拉」，拉到的清单仍要完整验签，被篡改的指针最多
//!   造成拒绝服务。
//! - 主机的 A、AAAA：DNS 失败或被污染时给拨号多一组地址。TLS 校验与 SPKI 钉扎仍按主机名，错的地址
//!   只是连不上。缓存、退避与地址排序在 `flare_core::client::dial`。
//!
//! 所以这里只做有界解析，任何格式问题都按「这个解析器没给出结果」处理。

use std::net::IpAddr;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use futures_util::future::join;

use super::fetcher::DOH_TIMEOUT_MS;
use super::{FetchRequest, HttpFetch};
use crate::parsed_url::ParsedUrl;

/// `Accept` 与响应类型。
pub const DNS_MESSAGE: &str = "application/dns-message";
/// 响应的体积上限：一条 512 字节的 UDP 报文都放得下的内容，这里已留足余量。
pub const MAX_RESPONSE_BYTES: usize = 4_096;
/// 一条 TXT 记录最多取几个镜像 URL。
pub const MAX_POINTER_URLS: usize = 8;
/// 查一个主机最多问几个解析器：依次问，有答复就停，后台查询不赶时间，解析器的压力要小。
pub const MAX_HOST_RESOLVERS: usize = 3;
/// 一个主机最多留几个地址。
const MAX_HOST_ADDRESSES: usize = 8;

const TYPE_A: u16 = 1;
const TYPE_TXT: u16 = 16;
const TYPE_AAAA: u16 = 28;
const CLASS_IN: u16 = 1;
const HEADER_LEN: usize = 12;
const POINTER_VERSION: &str = "v=flare1";

/// 主机地址的记录类型。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressRecord {
    A,
    Aaaa,
}

impl AddressRecord {
    fn code(self) -> u16 {
        match self {
            AddressRecord::A => TYPE_A,
            AddressRecord::Aaaa => TYPE_AAAA,
        }
    }
}

/// 一个主机的解析结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostAnswer {
    /// 至少一个
    pub addresses: Vec<IpAddr>,
    /// 答复里最小的 TTL（含 CNAME 链），秒
    pub ttl_sec: u32,
}

/// TXT 查询报文。
pub fn encode_query(name: &str) -> Option<Vec<u8>> {
    encode_question(name, TYPE_TXT)
}

/// A 或 AAAA 查询报文。
pub fn encode_address_query(name: &str, record: AddressRecord) -> Option<Vec<u8>> {
    encode_question(name, record.code())
}

/// DNS 查询报文（id 固定为 0，便于缓存，RFC 8484 §4.1）。
fn encode_question(name: &str, record_type: u16) -> Option<Vec<u8>> {
    let name = name.strip_suffix('.').unwrap_or(name);
    if name.is_empty() || name.len() > 253 {
        return None;
    }
    let mut query = Vec::with_capacity(HEADER_LEN + name.len() + 6);
    query.extend_from_slice(&[0, 0, 0x01, 0x00, 0, 1, 0, 0, 0, 0, 0, 0]);
    for label in name.split('.') {
        if label.is_empty() || label.len() > 63 {
            return None;
        }
        query.push(label.len() as u8);
        query.extend_from_slice(label.as_bytes());
    }
    query.push(0);
    query.extend_from_slice(&record_type.to_be_bytes());
    query.extend_from_slice(&CLASS_IN.to_be_bytes());
    Some(query)
}

/// `{resolver}?dns=<base64url>`。
fn url_of(resolver: &str, query: &[u8]) -> Option<String> {
    let mut url = ParsedUrl::parse(resolver).ok()?;
    url.append_query_pair("dns", &URL_SAFE_NO_PAD.encode(query));
    Some(url.to_string())
}

/// TXT 查询的 URL。
pub fn query_url(resolver: &str, name: &str) -> Option<String> {
    url_of(resolver, &encode_query(name)?)
}

/// A 或 AAAA 查询的 URL。
pub fn address_query_url(resolver: &str, name: &str, record: AddressRecord) -> Option<String> {
    url_of(resolver, &encode_address_query(name, record)?)
}

fn read_u16(message: &[u8], at: usize) -> Option<u16> {
    let bytes = message.get(at..at + 2)?;
    Some(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn read_u32(message: &[u8], at: usize) -> Option<u32> {
    let bytes = message.get(at..at + 4)?;
    Some(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

/// 跳过一个域名（可能以压缩指针结尾），返回之后的位置。
fn skip_name(message: &[u8], mut at: usize) -> Option<usize> {
    loop {
        let length = *message.get(at)?;
        match length {
            0 => return Some(at + 1),
            _ if length & 0xC0 == 0xC0 => return Some(at + 2),
            _ if length & 0xC0 != 0 => return None,
            _ => at += 1 + usize::from(length),
        }
    }
}

/// 答案区的一条记录。
struct Record<'a> {
    kind: u16,
    ttl_sec: u32,
    data: &'a [u8],
}

/// 响应答案区的全部记录。报文有问题或解析失败时为 `None`。
fn answers(message: &[u8]) -> Option<Vec<Record<'_>>> {
    if message.len() < HEADER_LEN || message[3] & 0x0F != 0 {
        return None;
    }
    let questions = read_u16(message, 4)?;
    let answers = read_u16(message, 6)?;
    let mut at = HEADER_LEN;
    for _ in 0..questions {
        at = skip_name(message, at)? + 4;
    }
    let mut records = Vec::new();
    for _ in 0..answers {
        at = skip_name(message, at)?;
        let kind = read_u16(message, at)?;
        let ttl_sec = read_u32(message, at + 4)?;
        let length = usize::from(read_u16(message, at + 8)?);
        let start = at + 10;
        let data = message.get(start..start + length)?;
        at = start + length;
        records.push(Record {
            kind,
            ttl_sec,
            data,
        });
    }
    Some(records)
}

/// 从响应里取全部 TXT 记录（一条记录的多个字符串拼起来）。报文有问题或解析失败时为 `None`。
pub fn parse_txt_response(message: &[u8]) -> Option<Vec<String>> {
    let mut records = Vec::new();
    for record in answers(message)? {
        if record.kind != TYPE_TXT {
            continue;
        }
        let rdata = record.data;
        let mut text = Vec::with_capacity(rdata.len());
        let mut offset = 0;
        while offset < rdata.len() {
            let part = usize::from(rdata[offset]);
            text.extend_from_slice(rdata.get(offset + 1..offset + 1 + part)?);
            offset += 1 + part;
        }
        records.push(String::from_utf8_lossy(&text).into_owned());
    }
    Some(records)
}

/// 从响应里取 A、AAAA。没有地址（包括 NXDOMAIN 与只有 CNAME）或报文有问题时为 `None`。
/// 答案区里的记录都算：解析器走完 CNAME 链，地址记录的名字本来就和问题不同。
pub fn parse_address_response(message: &[u8]) -> Option<HostAnswer> {
    let mut addresses = Vec::new();
    let mut ttl_sec = u32::MAX;
    for record in answers(message)? {
        ttl_sec = ttl_sec.min(record.ttl_sec);
        match record.kind {
            TYPE_A => {
                let octets: [u8; 4] = record.data.try_into().ok()?;
                addresses.push(IpAddr::from(octets));
            }
            TYPE_AAAA => {
                let octets: [u8; 16] = record.data.try_into().ok()?;
                addresses.push(IpAddr::from(octets));
            }
            _ => {}
        }
    }
    addresses.truncate(MAX_HOST_ADDRESSES);
    (!addresses.is_empty()).then_some(HostAnswer { addresses, ttl_sec })
}

/// A 与 AAAA 的结果交替排：拨号只取前几个，只有一种地址族能通的网络也要轮得到。
fn merge(v4: Option<HostAnswer>, v6: Option<HostAnswer>) -> Option<HostAnswer> {
    match (v4, v6) {
        (Some(v4), Some(v6)) => {
            let ttl_sec = v4.ttl_sec.min(v6.ttl_sec);
            let mut v4 = v4.addresses.into_iter();
            let mut v6 = v6.addresses.into_iter();
            let mut addresses = Vec::new();
            loop {
                let (first, second) = (v4.next(), v6.next());
                if first.is_none() && second.is_none() {
                    break;
                }
                addresses.extend(first);
                addresses.extend(second);
            }
            Some(HostAnswer { addresses, ttl_sec })
        }
        (answer, None) | (None, answer) => answer,
    }
}

async fn ask(
    http: &dyn HttpFetch,
    resolver: &str,
    host: &str,
    record: AddressRecord,
) -> Option<HostAnswer> {
    let request = FetchRequest {
        url: address_query_url(resolver, host, record)?,
        accept: Some(DNS_MESSAGE),
        timeout_ms: DOH_TIMEOUT_MS,
        max_body_bytes: MAX_RESPONSE_BYTES,
        ..FetchRequest::default()
    };
    match http.fetch(&request).await {
        Ok(response) if response.status == 200 => parse_address_response(&response.body),
        _ => None,
    }
}

/// 查一个主机的 A、AAAA：依次问解析器，第一个给出地址的为准，最多问 [`MAX_HOST_RESOLVERS`] 个。
pub async fn resolve_host(
    http: &dyn HttpFetch,
    resolvers: &[String],
    host: &str,
) -> Option<HostAnswer> {
    for resolver in resolvers.iter().take(MAX_HOST_RESOLVERS) {
        let (v4, v6) = join(
            ask(http, resolver, host, AddressRecord::A),
            ask(http, resolver, host, AddressRecord::Aaaa),
        )
        .await;
        if let Some(answer) = merge(v4, v6) {
            return Some(answer);
        }
    }
    None
}

/// `v=flare1;u=https://…/;u=https://…/` → 镜像 URL。版本不对或不是 https 的一律丢弃。
pub fn parse_pointer(record: &str) -> Vec<String> {
    let mut fields = record.split(';').map(str::trim);
    if fields.next() != Some(POINTER_VERSION) {
        return Vec::new();
    }
    fields
        .filter_map(|field| field.strip_prefix("u="))
        .filter(|value| ParsedUrl::parse(value).is_ok_and(|url| url.scheme() == "https"))
        .take(MAX_POINTER_URLS)
        .map(str::to_owned)
        .collect()
}

/// 假 DoH 服务用：把 TXT 文本编成响应报文。
#[doc(hidden)]
pub fn encode_txt_response(query: &[u8], records: &[&str]) -> Vec<u8> {
    let mut message = query.to_vec();
    message[2] = 0x81;
    message[3] = 0x80;
    message[6..8].copy_from_slice(&(records.len() as u16).to_be_bytes());
    for record in records {
        // 压缩指针指回问题里的域名。
        message.extend_from_slice(&[0xC0, HEADER_LEN as u8]);
        message.extend_from_slice(&TYPE_TXT.to_be_bytes());
        message.extend_from_slice(&CLASS_IN.to_be_bytes());
        message.extend_from_slice(&60u32.to_be_bytes());
        let chunks: Vec<&[u8]> = record.as_bytes().chunks(255).collect();
        let length: usize = chunks.iter().map(|chunk| chunk.len() + 1).sum();
        message.extend_from_slice(&(length as u16).to_be_bytes());
        for chunk in chunks {
            message.push(chunk.len() as u8);
            message.extend_from_slice(chunk);
        }
    }
    message
}

/// 假 DoH 服务用：把地址编成响应报文。
#[doc(hidden)]
pub fn encode_address_response(query: &[u8], addresses: &[IpAddr], ttl_sec: u32) -> Vec<u8> {
    let mut message = query.to_vec();
    message[2] = 0x81;
    message[3] = 0x80;
    message[6..8].copy_from_slice(&(addresses.len() as u16).to_be_bytes());
    for address in addresses {
        let (record_type, data) = match address {
            IpAddr::V4(ip) => (TYPE_A, ip.octets().to_vec()),
            IpAddr::V6(ip) => (TYPE_AAAA, ip.octets().to_vec()),
        };
        // 压缩指针指回问题里的域名。
        message.extend_from_slice(&[0xC0, HEADER_LEN as u8]);
        message.extend_from_slice(&record_type.to_be_bytes());
        message.extend_from_slice(&CLASS_IN.to_be_bytes());
        message.extend_from_slice(&ttl_sec.to_be_bytes());
        message.extend_from_slice(&(data.len() as u16).to_be_bytes());
        message.extend_from_slice(&data);
    }
    message
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use super::super::{FetchError, FetchResponse};
    use super::*;

    const NAME: &str = "_flare.b7f3c9a1e2d4b6a8.example-boot.org";
    const HOST: &str = "im-ct.example.com";
    const RESOLVER_A: &str = "https://1.1.1.1/dns-query";
    const RESOLVER_B: &str = "https://223.5.5.5/dns-query";

    fn ip(text: &str) -> IpAddr {
        text.parse().unwrap()
    }

    fn resolvers(list: &[&str]) -> Vec<String> {
        list.iter().map(|resolver| (*resolver).to_owned()).collect()
    }

    /// 假解析器：按 URL 应答，没有配置的连不上。
    #[derive(Default)]
    struct FakeResolvers {
        routes: Mutex<HashMap<String, Vec<u8>>>,
        log: Mutex<Vec<FetchRequest>>,
    }

    impl FakeResolvers {
        fn answer(&self, resolver: &str, record: AddressRecord, addresses: &[&str], ttl_sec: u32) {
            let addresses: Vec<IpAddr> = addresses.iter().map(|text| ip(text)).collect();
            let query = encode_address_query(HOST, record).unwrap();
            self.routes.lock().unwrap().insert(
                address_query_url(resolver, HOST, record).unwrap(),
                encode_address_response(&query, &addresses, ttl_sec),
            );
        }

        fn asked(&self) -> Vec<String> {
            self.log
                .lock()
                .unwrap()
                .iter()
                .map(|request| request.url.clone())
                .collect()
        }
    }

    #[async_trait::async_trait]
    impl HttpFetch for FakeResolvers {
        async fn fetch(&self, request: &FetchRequest) -> Result<FetchResponse, FetchError> {
            self.log.lock().unwrap().push(request.clone());
            match self.routes.lock().unwrap().get(&request.url).cloned() {
                Some(body) => Ok(FetchResponse {
                    status: 200,
                    etag: None,
                    body,
                }),
                None => Err(FetchError::Unreachable),
            }
        }
    }

    #[test]
    fn query_matches_the_wire_format() {
        let query = encode_query("a.bc").unwrap();
        assert_eq!(
            query,
            [
                0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, b'a', 2, b'b', b'c', 0, 0, 16, 0, 1
            ]
        );
        assert_eq!(encode_query("a..b"), None);
        assert_eq!(encode_query(&"a".repeat(300)), None);
        let url = query_url("https://1.1.1.1/dns-query", "a.bc").unwrap();
        assert!(url.starts_with("https://1.1.1.1/dns-query?dns=AAABAAAB"));
    }

    #[test]
    fn answers_round_trip_including_long_records() {
        let query = encode_query(NAME).unwrap();
        let long = format!("v=flare1;u=https://{}.example.net/", "m".repeat(300));
        let response = encode_txt_response(&query, &["v=flare1;u=https://a.example.net/", &long]);
        let records = parse_txt_response(&response).unwrap();
        assert_eq!(records[0], "v=flare1;u=https://a.example.net/");
        assert_eq!(records[1], long);
    }

    #[test]
    fn bad_messages_yield_nothing() {
        let query = encode_query(NAME).unwrap();
        let mut failure = encode_txt_response(&query, &["v=flare1"]);
        failure[3] = 0x83;
        assert_eq!(parse_txt_response(&failure), None);
        let good = encode_txt_response(&query, &["v=flare1"]);
        for cut in [5, HEADER_LEN, good.len() - 1] {
            assert_eq!(parse_txt_response(&good[..cut]), None, "cut at {cut}");
        }
        assert_eq!(parse_txt_response(&[]), None);
    }

    #[test]
    fn address_queries_differ_from_txt_only_in_the_type() {
        assert_eq!(
            encode_address_query("a.bc", AddressRecord::A).unwrap(),
            [
                0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, b'a', 2, b'b', b'c', 0, 0, 1, 0, 1
            ]
        );
        assert_eq!(
            encode_address_query("a.bc", AddressRecord::Aaaa).unwrap(),
            [
                0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, b'a', 2, b'b', b'c', 0, 0, 28, 0, 1
            ]
        );
        let url = address_query_url(RESOLVER_A, "a.bc", AddressRecord::A).unwrap();
        assert!(url.starts_with("https://1.1.1.1/dns-query?dns=AAABAAAB"));
    }

    /// 手写的线上格式：`a.bc` 是 CNAME 到 `x.a.bc`，地址记录挂在别名上，TTL 取链上最小的。
    #[test]
    fn an_address_behind_a_cname_is_read_with_the_smallest_ttl() {
        let message = [
            // 头：应答、1 个问题、2 条答案
            0x00, 0x00, 0x81, 0x80, 0x00, 0x01, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
            // 问题：a.bc A IN，从第 12 字节起
            0x01, b'a', 0x02, b'b', b'c', 0x00, 0x00, 0x01, 0x00, 0x01,
            // CNAME，TTL 300，内容 x.a.bc，内容从第 34 字节起
            0xC0, 0x0C, 0x00, 0x05, 0x00, 0x01, 0x00, 0x00, 0x01, 0x2C, 0x00, 0x04, 0x01, b'x',
            0xC0, 0x0C, // A，名字指向第 34 字节，TTL 60
            0xC0, 0x22, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x3C, 0x00, 0x04, 198, 51, 100,
            7,
        ];
        assert_eq!(
            parse_address_response(&message),
            Some(HostAnswer {
                addresses: vec![ip("198.51.100.7")],
                ttl_sec: 60,
            })
        );
    }

    #[test]
    fn address_answers_round_trip() {
        let query = encode_address_query(HOST, AddressRecord::Aaaa).unwrap();
        let addresses = [ip("2001:db8::7"), ip("2001:db8::8")];
        let response = encode_address_response(&query, &addresses, 120);
        assert_eq!(
            parse_address_response(&response),
            Some(HostAnswer {
                addresses: addresses.to_vec(),
                ttl_sec: 120,
            })
        );
    }

    #[test]
    fn bad_address_answers_yield_nothing() {
        let query = encode_address_query(HOST, AddressRecord::A).unwrap();
        // 没有地址、NXDOMAIN、截断、A 记录长度不对
        assert_eq!(
            parse_address_response(&encode_address_response(&query, &[], 60)),
            None
        );
        let good = encode_address_response(&query, &[ip("198.51.100.7")], 60);
        let mut failure = good.clone();
        failure[3] = 0x83;
        assert_eq!(parse_address_response(&failure), None);
        assert_eq!(parse_address_response(&good[..good.len() - 1]), None);
        let mut wrong_length = good.clone();
        let length_low_byte = good.len() - 5;
        wrong_length[length_low_byte] = 3;
        wrong_length.pop();
        assert_eq!(parse_address_response(&wrong_length), None);
    }

    #[test]
    fn long_answers_are_bounded() {
        let query = encode_address_query(HOST, AddressRecord::A).unwrap();
        let addresses: Vec<IpAddr> = (0..20)
            .map(|last| IpAddr::from([198, 51, 100, last]))
            .collect();
        let answer = parse_address_response(&encode_address_response(&query, &addresses, 60));
        assert_eq!(answer.unwrap().addresses.len(), MAX_HOST_ADDRESSES);
    }

    #[tokio::test]
    async fn both_families_are_asked_and_interleaved() {
        let net = FakeResolvers::default();
        net.answer(
            RESOLVER_A,
            AddressRecord::A,
            &["198.51.100.7", "198.51.100.8"],
            300,
        );
        net.answer(RESOLVER_A, AddressRecord::Aaaa, &["2001:db8::7"], 60);
        let answer = resolve_host(&net, &resolvers(&[RESOLVER_A]), HOST).await;
        assert_eq!(
            answer,
            Some(HostAnswer {
                addresses: vec![ip("198.51.100.7"), ip("2001:db8::7"), ip("198.51.100.8"),],
                ttl_sec: 60,
            })
        );
        let request = net.log.lock().unwrap()[0].clone();
        assert_eq!(request.accept, Some(DNS_MESSAGE));
        assert_eq!(request.timeout_ms, DOH_TIMEOUT_MS);
        assert_eq!(request.max_body_bytes, MAX_RESPONSE_BYTES);
    }

    #[tokio::test]
    async fn one_family_is_enough() {
        let net = FakeResolvers::default();
        net.answer(RESOLVER_A, AddressRecord::Aaaa, &["2001:db8::7"], 60);
        let answer = resolve_host(&net, &resolvers(&[RESOLVER_A]), HOST).await;
        assert_eq!(answer.unwrap().addresses, [ip("2001:db8::7")]);
    }

    #[tokio::test]
    async fn the_next_resolver_is_asked_only_after_a_failure() {
        let net = FakeResolvers::default();
        net.answer(RESOLVER_B, AddressRecord::A, &["198.51.100.7"], 60);
        let list = resolvers(&[RESOLVER_A, RESOLVER_B]);
        let answer = resolve_host(&net, &list, HOST).await;
        assert_eq!(answer.unwrap().addresses, [ip("198.51.100.7")]);
        assert_eq!(net.asked().len(), 4);

        let net = FakeResolvers::default();
        net.answer(RESOLVER_A, AddressRecord::A, &["198.51.100.7"], 60);
        resolve_host(&net, &list, HOST).await.unwrap();
        assert_eq!(net.asked().len(), 2, "第一个解析器答了就不再问下一个");
        assert!(net.asked().iter().all(|url| url.starts_with(RESOLVER_A)));
    }

    #[tokio::test]
    async fn a_lookup_asks_a_bounded_number_of_resolvers() {
        let net = FakeResolvers::default();
        let list: Vec<String> = (0..8)
            .map(|index| format!("https://dns-{index}.example.com/dns-query"))
            .collect();
        assert_eq!(resolve_host(&net, &list, HOST).await, None);
        assert_eq!(net.asked().len(), MAX_HOST_RESOLVERS * 2);
    }

    #[test]
    fn pointers_keep_only_https_mirrors_of_a_known_version() {
        assert_eq!(
            parse_pointer("v=flare1; u=https://a.example.net/flare/ ;u=http://b.example.net/;x=1"),
            vec!["https://a.example.net/flare/".to_owned()]
        );
        assert!(parse_pointer("v=spf1 include:example.net").is_empty());
        assert!(parse_pointer("v=flare2;u=https://a.example.net/").is_empty());
    }
}
